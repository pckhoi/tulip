package tulip

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgtype"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
	"github.com/mmcloughlin/meow"
	"go.uber.org/zap"
)

const (
	DefaultTableName    = "tulip_rule"
	DefaultDatabaseName = "tulip"
	DefaultTimeout      = time.Second * 10
	DefaultSyncPeriod   = time.Second * 60
)

// Manager manages access control policies.
type Manager struct {
	pool            *pgxpool.Pool
	tableName       string
	dbName          string
	skipDBCreate    bool
	timeout         time.Duration
	syncInterval    time.Duration
	skipTableCreate bool
	matcher         Matcher
	p               Policies
	g               Policies
	mutex           sync.Mutex
	nConn           *pgx.Conn
	done            chan bool
	ticker          *time.Ticker
	logger          *zap.Logger
}

type Option func(m *Manager)

// NewManager creates a new manager with connection conn which must either be a PostgreSQL
// connection string or an instance of *pgx.ConnConfig from package github.com/jackc/pgx/v4.
func NewManager(conn interface{}, matcher Matcher, opts ...Option) (*Manager, error) {
	m := &Manager{
		dbName:       DefaultDatabaseName,
		tableName:    DefaultTableName,
		timeout:      DefaultTimeout,
		syncInterval: DefaultSyncPeriod,
		matcher:      matcher,
		done:         make(chan bool),
	}
	for _, opt := range opts {
		opt(m)
	}
	var err error
	if m.skipDBCreate {
		m.pool, err = connectDatabase(m.dbName, conn)
		if err != nil {
			return nil, fmt.Errorf("tulip.NewManager: %v", err)
		}
	} else {
		m.pool, err = createDatabase(m.dbName, conn)
		if err != nil {
			return nil, fmt.Errorf("tulip.NewManager: %v", err)
		}
	}
	if !m.skipTableCreate {
		if err := m.createTable(); err != nil {
			return nil, fmt.Errorf("tulip.NewManager: %v", err)
		}
	}
	if err = m.createTrigger(); err != nil {
		return nil, fmt.Errorf("tulip.NewManager: %v", err)
	}
	go m.listen()
	if err = m.LoadPolicies(); err != nil {
		return nil, fmt.Errorf("tulip.NewManager: %v", err)
	}
	m.ticker = time.NewTicker(m.syncInterval)
	go m.periodicallyRefreshPolicies()
	return m, nil
}

// WithTableName can be used to pass custom table name for Tulip rules
func WithTableName(tableName string) Option {
	return func(m *Manager) {
		m.tableName = tableName
	}
}

// WithSkipTableCreate skips the table creation step when the manager starts
// If the Tulip rules table does not exist, it will lead to issues when using the manager
func WithSkipTableCreate() Option {
	return func(m *Manager) {
		m.skipTableCreate = true
	}
}

// WithTableName can be used to pass custom database name for Tulip rules
func WithDatabase(dbname string) Option {
	return func(m *Manager) {
		m.dbName = dbname
	}
}

func WithSkipDatabaseCreate() Option {
	return func(m *Manager) {
		m.skipDBCreate = true
	}
}

// WithTimeout specifies a Postgres connection timeout for the manager
func WithTimeout(timeout time.Duration) Option {
	return func(m *Manager) {
		m.timeout = timeout
	}
}

// WithSyncInterval specifies a different sync interval for the manager
func WithSyncInterval(interval time.Duration) Option {
	return func(m *Manager) {
		m.syncInterval = interval
	}
}

// WithZapLogger specifies a logger for the manager
func WithZapLogger(logger *zap.Logger) Option {
	return func(m *Manager) {
		m.logger = logger
	}
}

func (m *Manager) PolicyCount() int {
	return m.p.Len()
}

func (m *Manager) GroupingPolicyCount() int {
	return m.g.Len()
}

func (m *Manager) periodicallyRefreshPolicies() {
	for {
		select {
		case <-m.done:
			return
		case <-m.ticker.C:
			if m.logger != nil {
				m.logger.Debug("policies before refresh",
					zap.Int("policy_count", len(m.p)),
					zap.Int("group_count", len(m.g)),
				)
			}
			if err := m.LoadPolicies(); err != nil {
				if m.logger != nil {
					m.logger.Error("error while refreshing policies",
						zap.Error(err),
					)
				}
			}
		}
	}
}

// LoadPolicies loads policies from database.
func (m *Manager) LoadPolicies() error {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	var pType, v0, v1, v2, v3, v4, v5 pgtype.Text
	m.p = m.p[:0]
	m.g = m.g[:0]
	_, err := m.pool.QueryFunc(
		ctx,
		fmt.Sprintf(`SELECT "p_type", "v0", "v1", "v2", "v3", "v4", "v5" FROM %s`, m.tableName),
		nil,
		[]interface{}{&pType, &v0, &v1, &v2, &v3, &v4, &v5},
		func(pgx.QueryFuncRow) error {
			switch pType.String {
			case "p":
				m.p = append(m.p, []string{v0.String, v1.String, v2.String, v3.String, v4.String, v5.String})
			case "g":
				m.g = append(m.g, []string{v0.String, v1.String, v2.String, v3.String, v4.String, v5.String})
			}
			return nil
		},
	)
	if err != nil {
		return err
	}
	sort.Sort(m.p)
	sort.Sort(m.g)
	if m.logger != nil {
		m.logger.Debug("loaded policies",
			zap.Int("policy_count", len(m.p)),
			zap.Int("group_count", len(m.g)),
		)
	}
	return nil
}

func policyID(ptype string, rule []string) string {
	end := len(rule)
	for i, s := range rule {
		if s == "" {
			end = i
			break
		}
	}
	data := strings.Join(append([]string{ptype}, rule[:end]...), ",")
	sum := meow.Checksum(0, []byte(data))
	return fmt.Sprintf("%x", sum)
}

func policyArgs(ptype string, rule []string) []interface{} {
	row := make([]interface{}, 8)
	row[0] = pgtype.Text{
		String: policyID(ptype, rule),
		Status: pgtype.Present,
	}
	row[1] = pgtype.Text{
		String: ptype,
		Status: pgtype.Present,
	}
	l := len(rule)
	for i := 0; i < 6; i++ {
		if i < l {
			if rule[i] == "" {
				panic(fmt.Errorf("can't insert policy with empty value: ptype was %q, rule was %v", ptype, rule))
			}
			row[2+i] = pgtype.Text{
				String: rule[i],
				Status: pgtype.Present,
			}
		} else {
			row[2+i] = pgtype.Text{
				Status: pgtype.Null,
			}
		}
	}
	return row
}

func (m *Manager) insertPolicyStmt() string {
	return fmt.Sprintf(`
		INSERT INTO %s (id, p_type, v0, v1, v2, v3, v4, v5)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT ON CONSTRAINT %s_pkey DO NOTHING
	`, m.tableName, m.tableName)
}

// AddPolicy adds a policy rule to the storage.
func (m *Manager) AddPolicy(ptype string, rule []string) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	_, err := m.pool.Exec(ctx,
		m.insertPolicyStmt(),
		policyArgs(ptype, rule)...,
	)
	return err
}

// AddPolicies adds policy rules to the storage.
func (m *Manager) AddPolicies(pRules, gRules [][]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	return m.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		for _, rule := range pRules {
			b.Queue(m.insertPolicyStmt(), policyArgs("p", rule)...)
		}
		for _, rule := range gRules {
			b.Queue(m.insertPolicyStmt(), policyArgs("g", rule)...)
		}
		br := tx.SendBatch(context.Background(), b)
		defer br.Close()
		for range append(pRules, gRules...) {
			_, err := br.Exec()
			if err != nil {
				return err
			}
		}
		return br.Close()
	})
}

// RemovePolicy removes a policy rule from the storage.
func (m *Manager) RemovePolicy(ptype string, rule []string) error {
	id := policyID(ptype, rule)
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	_, err := m.pool.Exec(ctx,
		fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.tableName),
		id,
	)
	return err
}

// RemovePolicies removes policy rules from the storage.
func (m *Manager) RemovePolicies(pRules, gRules [][]string) error {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	return m.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		for _, rule := range pRules {
			id := policyID("p", rule)
			b.Queue(fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.tableName), id)
		}
		for _, rule := range gRules {
			id := policyID("g", rule)
			b.Queue(fmt.Sprintf("DELETE FROM %s WHERE id = $1", m.tableName), id)
		}
		br := tx.SendBatch(context.Background(), b)
		defer br.Close()
		for range append(pRules, gRules...) {
			_, err := br.Exec()
			if err != nil {
				return err
			}
		}
		return br.Close()
	})
}

func (m *Manager) RemoveFilteredPolicies(pPattern, gPattern []string) error {
	var pRules, gRules [][]string
	if pPattern != nil {
		pRules = m.Filter(pPattern...)
	}
	if gPattern != nil {
		gRules = m.FilterGroups(gPattern...)
	}
	return m.RemovePolicies(pRules, gRules)
}

// Close closes all connections and stops all goroutines
func (m *Manager) Close() error {
	m.ticker.Stop()
	if m.done != nil {
		// send done signal to both go routines
		m.done <- true
		m.done <- true
		m.done = nil
	}
	if m.pool != nil {
		m.pool.Close()
		m.pool = nil
	}
	if m.nConn != nil {
		if err := m.nConn.Close(context.Background()); err != nil {
			return err
		}
		m.nConn = nil
	}
	return nil
}

func (m *Manager) createTable() error {
	if m.logger != nil {
		m.logger.Info("creating table", zap.String("table_name", m.tableName))
	}
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	_, err := m.pool.Exec(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id text PRIMARY KEY,
			p_type text,
			v0 text,
			v1 text,
			v2 text,
			v3 text,
			v4 text,
			v5 text
		)
	`, m.tableName))
	return err
}

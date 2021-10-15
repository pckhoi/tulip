package tulip

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v4"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zaptest"
)

func dropDB(t *testing.T, dbname string) {
	t.Helper()
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, os.Getenv("PG_CONN"))
	require.NoError(t, err)
	_, err = conn.Exec(ctx, "DROP DATABASE "+dbname)
	require.NoError(t, err)
	require.NoError(t, conn.Close(ctx))
}

func retryUntil(t *testing.T, d time.Duration, max int, cond func() bool, msg string) {
	t.Helper()
	for i := 0; i < max; i++ {
		if cond() {
			return
		}
		time.Sleep(d)
	}
	t.Fatalf("max retry exceed: %s", msg)
}

func waitForNotification(t *testing.T, m *Manager, policyCount, groupCount int) {
	t.Helper()
	retryUntil(t, 100*time.Millisecond, 10, func() bool {
		return len(m.p) == policyCount && len(m.g) == groupCount
	}, "waiting for notification")
}

func testAddPolicy(t *testing.T, connStr string, opts []Option) {
	opts = append(opts,
		WithTableName(BrokenRandomLowerAlphaString(5)),
		WithZapLogger(zaptest.NewLogger(t)),
	)
	m, err := NewManager(connStr, RBACWithDomain, opts...)
	require.NoError(t, err)
	defer m.Close()

	require.NoError(t, m.AddPolicy("p", []string{"alice", "uni", "class_a", "teach"}))
	waitForNotification(t, m, 1, 0)
	assert.True(t, m.Enforce("alice", "uni", "class_a", "teach"))

	require.NoError(t, m.AddPolicies(
		[][]string{
			{"teacher", "uni", "class_a", "teach"},
			{"teacher", "uni", "class_b", "teach"},
		},
		[][]string{
			{"aaron", "teacher", "uni"},
			{"adam", "teacher", "uni"},
		},
	))
	waitForNotification(t, m, 3, 2)
	assert.True(t, m.Enforce("aaron", "uni", "class_a", "teach"))
	assert.True(t, m.Enforce("adam", "uni", "class_b", "teach"))

	require.NoError(t, m.RemovePolicy("p", []string{"alice", "uni", "class_a", "teach"}))
	waitForNotification(t, m, 2, 2)
	assert.False(t, m.Enforce("alice", "uni", "class_a", "teach"))

	require.NoError(t, m.RemovePolicies(
		[][]string{
			{"teacher", "uni", "class_a", "teach"},
		},
		[][]string{
			{"aaron", "teacher", "uni"},
		},
	))
	waitForNotification(t, m, 1, 1)
	assert.False(t, m.Enforce("aaron", "uni", "class_a", "teach"))
	assert.True(t, m.Enforce("adam", "uni", "class_b", "teach"))
}

func testFilter(t *testing.T, connStr string, opts []Option) {
	opts = append(opts,
		WithTableName(BrokenRandomLowerAlphaString(5)),
		WithZapLogger(zaptest.NewLogger(t)),
	)
	m, err := NewManager(connStr, RBACWithDomain, opts...)
	require.NoError(t, err)
	defer m.Close()

	require.NoError(t, m.AddPolicies(
		[][]string{
			{"a", "b", "c"},
			{"a", "b", "d"},
			{"b", "a", "d"},
		},
		[][]string{
			{"a", "b", "c"},
			{"a", "d", "c"},
			{"b", "e", "f"},
			{"a", "f", "g"},
		},
	))
	waitForNotification(t, m, 3, 4)

	assert.NotNil(t, m.FindExact("a", "b", "d"))
	assert.Nil(t, m.FindExact("a", "c", "d"))

	assert.Equal(t, Policies([][]string{
		{"a", "b", "c", "", "", ""},
		{"a", "d", "c", "", "", ""},
	}), m.FilterGroups("a", "", "c"))

	assert.Equal(t, Policies([][]string{
		{"b", "a", "d", "", "", ""},
	}), m.FilterWithGroups(0, Policies([][]string{
		{"a", "b", "c"},
		{"b", "e", "f"},
	}), 1))
}

func TestManager(t *testing.T) {
	connStr := os.Getenv("PG_CONN")
	require.NotEmpty(t, connStr, "must run with non-empty PG_CONN")
	dbName := "test_tulip"
	pool, err := createDatabase(dbName, connStr)
	require.NoError(t, err)
	pool.Close()
	defer dropDB(t, dbName)
	opts := []Option{WithDatabase(dbName), WithSkipDatabaseCreate()}

	type subtest struct {
		Name string
		F    func(t *testing.T, connStr string, opts []Option)
	}

	t.Run("", func(t *testing.T) {
		for _, st := range []subtest{
			{"AddPolicy", testAddPolicy},
			{"Filter", testFilter},
		} {
			st := st
			t.Run(st.Name, func(t *testing.T) {
				t.Parallel()
				st.F(t, connStr, opts)
			})
		}
	})
}

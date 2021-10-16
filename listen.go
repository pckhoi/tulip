package tulip

import (
	"context"
	"encoding/json"
	"fmt"
	"io"

	"github.com/jackc/pgx/v4"
	"go.uber.org/zap"
)

func (m *Manager) createTrigger() error {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	return m.pool.BeginFunc(ctx, func(tx pgx.Tx) error {
		b := &pgx.Batch{}
		b.Queue(fmt.Sprintf("DROP TRIGGER IF EXISTS notify_%s ON %s", m.tableName, m.tableName))
		b.Queue(fmt.Sprintf(`
			create or replace function tg_notify_%s ()
			returns trigger
			language plpgsql
			as $$
				declare
					channel text := TG_ARGV[0];
				begin
					IF (TG_OP = 'DELETE') THEN
						PERFORM (
							with payload(op, p_type, rule) as
							(
								select TG_OP, OLD.p_type, ARRAY[OLD.v0, OLD.v1, OLD.v2, OLD.v3, OLD.v4, OLD.v5]
							)
							select pg_notify(channel, row_to_json(payload)::text)
							from payload
						);
					ELSIF (TG_OP = 'INSERT') THEN
						PERFORM (
							with payload(op, p_type, rule) as
							(
								select TG_OP, NEW.p_type, ARRAY[NEW.v0, NEW.v1, NEW.v2, NEW.v3, NEW.v4, NEW.v5]
							)
							select pg_notify(channel, row_to_json(payload)::text)
							from payload
						);
					END IF;
					RETURN NULL;
				end;
			$$
		`, m.tableName))
		b.Queue(fmt.Sprintf(`
			CREATE TRIGGER notify_%s
			AFTER INSERT OR DELETE
			ON %s
			FOR EACH ROW
			EXECUTE PROCEDURE tg_notify_%s('%s_rules')
		`, m.tableName, m.tableName, m.tableName, m.tableName))
		br := tx.SendBatch(context.Background(), b)
		defer br.Close()
		for i := 0; i < b.Len(); i++ {
			_, err := br.Exec()
			if err != nil {
				return err
			}
		}
		return br.Close()
	})
}

type policyNotification struct {
	Op    string   `json:"op"`
	PType string   `json:"p_type"`
	Rule  []string `json:"rule"`
}

func (m *Manager) listen() {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	var err error
	m.nConn, err = pgx.ConnectConfig(ctx, m.pool.Config().ConnConfig)
	if err != nil {
		panic(err)
	}
	_, err = m.nConn.Exec(ctx, fmt.Sprintf("listen %s_rules", m.tableName))
	if err != nil {
		panic(err)
	}
	ch := make(chan policyNotification, 16)
	go func() {
		for {
			notification, err := m.nConn.WaitForNotification(context.Background())
			if err != nil {
				if err == io.ErrUnexpectedEOF && m.nConn.IsClosed() {
					return
				}
				if m.logger != nil {
					m.logger.Error("error waiting for notification",
						zap.Error(err),
					)
				}
			}
			obj := policyNotification{}
			err = json.Unmarshal([]byte(notification.Payload), &obj)
			if err != nil {
				if m.logger != nil {
					m.logger.Error("error unmarshaling json",
						zap.Error(err),
					)
				}
			}
			ch <- obj
		}
	}()
	for {
		select {
		case <-m.done:
			return
		case obj := <-ch:
			if m.logger != nil {
				m.logger.Debug("receive pg notification",
					zap.String("op", obj.Op),
					zap.String("ptype", obj.PType),
					zap.Strings("rule", obj.Rule),
				)
			}
			m.mutex.Lock()
			switch obj.Op {
			case "INSERT":
				switch obj.PType {
				case "p":
					m.p.Insert(obj.Rule)
				case "g":
					m.g.Insert(obj.Rule)
				}
			case "DELETE":
				switch obj.PType {
				case "p":
					m.p.Remove(obj.Rule)
				case "g":
					m.g.Remove(obj.Rule)
				}
			}
			m.mutex.Unlock()
		}
	}
}

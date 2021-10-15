package tulip

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

func connectDatabase(dbname string, arg interface{}) (*pgxpool.Pool, error) {
	var cfg *pgx.ConnConfig
	var err error
	switch v := arg.(type) {
	case string:
		cfg, err = pgx.ParseConfig(v)
		if err != nil {
			return nil, err
		}
	case *pgx.ConnConfig:
		cfg = v
	default:
		return nil, fmt.Errorf("must pass in a PostgreS URL string or an instance of *pgx.ConnConfig, received %T instead", arg)
	}
	cfg.Database = dbname
	pcfg, err := pgxpool.ParseConfig(cfg.ConnString())
	if err != nil {
		return nil, err
	}
	pcfg.ConnConfig.Database = dbname
	return pgxpool.ConnectConfig(context.Background(), pcfg)
}

func createDatabase(dbname string, arg interface{}) (*pgxpool.Pool, error) {
	var conn *pgx.Conn
	var err error
	ctx := context.Background()
	switch v := arg.(type) {
	case string:
		conn, err = pgx.Connect(ctx, v)
		if err != nil {
			return nil, err
		}
	case *pgx.ConnConfig:
		conn, err = pgx.ConnectConfig(ctx, v)
		if err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("must pass in a PostgreS URL string or an instance of *pgx.ConnConfig, received %T instead", arg)
	}

	rows, err := conn.Query(ctx, "SELECT FROM pg_database WHERE datname = $1", dbname)
	if err != nil {
		return nil, err
	}
	createdb := !rows.Next()
	rows.Close()

	if createdb {
		_, err = conn.Exec(ctx, "CREATE DATABASE "+dbname)
		if err != nil {
			return nil, err
		}
	}
	if err := conn.Close(ctx); err != nil {
		return nil, err
	}

	config := conn.Config()
	config.Database = dbname
	if createdb {
		conn, err = pgx.ConnectConfig(ctx, config)
		if err != nil {
			return nil, err
		}
		_, err = conn.Exec(ctx, "create domain uint64 as numeric(20,0)")
		if err != nil {
			return nil, err
		}
		if err := conn.Close(ctx); err != nil {
			return nil, err
		}
	}

	cfg, err := pgxpool.ParseConfig(config.ConnString())
	if err != nil {
		return nil, err
	}
	cfg.ConnConfig.Database = dbname
	return pgxpool.ConnectConfig(ctx, cfg)
}

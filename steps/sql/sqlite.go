package sql

import (
	"context"
	"database/sql"
	"fmt"
	"go-etl/core"
	"go-etl/pipeline"

	_ "github.com/mattn/go-sqlite3"
)

type SQLiteStep struct {
	name       string
	connection string `config:""`
	query      string `config:""`
}

func (s *SQLiteStep) Name() string { return s.name }

func (s *SQLiteStep) Run(ctx context.Context, state *core.PipelineState) (map[string]*core.Data, error) {
	db, err := sql.Open("sqlite3", s.connection)
	if err != nil {
		return nil, fmt.Errorf("open db error: %w", err)
	}
	defer db.Close()

	rows, err := db.Query(s.query)
	if err != nil {
		return nil, fmt.Errorf("query error: %w", err)
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, fmt.Errorf("get columns: %w", err)
	}

	var results []map[string]interface{}

	for rows.Next() {
		vals := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return nil, err
		}

		rowMap := make(map[string]interface{})
		for i, col := range cols {
			rowMap[col] = vals[i]
		}

		results = append(results, rowMap)
	}

	return map[string]*core.Data{
		"default": {Value: results},
	}, nil
}

func init() {
	pipeline.RegisterStepType("sqlite", func(name string, config map[string]any) (core.Step, error) {
		connection, ok := config["connection"].(string)
		if !ok {
			return nil, core.ErrMissingConfig("connection")
		}

		query, ok := config["query"].(string)
		if !ok {
			return nil, core.ErrMissingConfig("query")
		}

		return &SQLiteStep{name: name, connection: connection, query: query}, nil
	})
}

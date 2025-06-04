package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"

	sdk "go-etl-sdk"

	_ "github.com/denisenkom/go-mssqldb"
)

type PluginInput struct {
	Connection string `json:"connection"`
	Query      string `json:"query"`
}

type PluginOutput struct {
	Value string `json:"value"`
	Error error  `json:"error"`
}

func main() {
	input, err := sdk.ReadInput[PluginInput]()
	if err != nil {
		fmt.Fprintf(os.Stderr, "read error: %v\n", err)
		os.Exit(1)
	}

	db, err := sql.Open("sqlserver", input.Connection)
	if err != nil {
		return
	}
	defer db.Close()

	ctx := context.Background()
	rows, err := db.QueryContext(ctx, input.Query)
	if err != nil {
		return
	}
	defer rows.Close()

	cols, err := rows.Columns()
	if err != nil {
		return
	}

	results := []map[string]interface{}{}

	for rows.Next() {
		values := make([]interface{}, len(cols))
		ptrs := make([]interface{}, len(cols))
		for i := range values {
			ptrs[i] = &values[i]
		}

		if err := rows.Scan(ptrs...); err != nil {
			return
		}

		rowMap := make(map[string]interface{})
		for i, col := range cols {
			rowMap[col] = values[i]
		}
		results = append(results, rowMap)
	}

	sdk.WriteOutput(results)
}

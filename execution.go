package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	. "github.com/cube2222/octosql/execution"
	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
)

type DatasourceExecuting struct {
	fields []physical.SchemaField
	table  string

	placeholderExprs []Expression
	db               *sql.DB
	stmt             *sql.Stmt
}

func (d *DatasourceExecuting) Run(ctx ExecutionContext, produce ProduceFn, metaSend MetaSendFn) error {
	placeholderValues := make([]interface{}, len(d.placeholderExprs))
	for i := range d.placeholderExprs {
		value, err := d.placeholderExprs[i].Evaluate(ctx)
		if err != nil {
			return fmt.Errorf("couldn't evaluate pushed-down predicate placeholder expression: %w", err)
		}
		// TODO: Use internal function for this.
		placeholderValues[i] = value.ToRawGoValue()
	}

	rows, err := d.stmt.QueryContext(ctx, placeholderValues...)
	if err != nil {
		return fmt.Errorf("couldn't execute database query: %w", err)
	}

	for rows.Next() {
		values := make([]interface{}, len(d.fields))
		references := make([]interface{}, len(d.fields))
		for i := range references {
			references[i] = &values[i]
		}
		err := rows.Scan(references...)
		if err != nil {
			return fmt.Errorf("couldn't get row values: %w", err)
		}
		recordValues := make([]octosql.Value, len(values))
		for i, value := range values {
			switch value := value.(type) {
			case int:
				recordValues[i] = octosql.NewInt(value)
			case int8:
				recordValues[i] = octosql.NewInt(int(value))
			case int16:
				recordValues[i] = octosql.NewInt(int(value))
			case int32:
				recordValues[i] = octosql.NewInt(int(value))
			case int64:
				recordValues[i] = octosql.NewInt(int(value))
			case uint8:
				recordValues[i] = octosql.NewInt(int(value))
			case uint16:
				recordValues[i] = octosql.NewInt(int(value))
			case uint32:
				recordValues[i] = octosql.NewInt(int(value))
			case uint64:
				recordValues[i] = octosql.NewInt(int(value))
			case bool:
				recordValues[i] = octosql.NewBoolean(value)
			case float32:
				recordValues[i] = octosql.NewFloat(float64(value))
			case float64:
				recordValues[i] = octosql.NewFloat(value)
			case string:
				recordValues[i] = octosql.NewString(value)
			case time.Time:
				recordValues[i] = octosql.NewTime(value)
			case nil:
				recordValues[i] = octosql.NewNull()
			case []byte:
				recordValues[i] = octosql.NewString(string(value))
			default:
				log.Printf("unknown mysql value type, setting null: %T, %+v", value, value)
				recordValues[i] = octosql.NewNull()
			}
		}
		if err := produce(ProduceFromExecutionContext(ctx), NewRecord(recordValues, false, time.Time{})); err != nil {
			return fmt.Errorf("couldn't produce record: %w", err)
		}
	}
	return nil
}

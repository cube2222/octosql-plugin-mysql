package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"

	"github.com/cube2222/octosql/octosql"
	"github.com/cube2222/octosql/physical"
	"github.com/cube2222/octosql/plugins"
)

type Config struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Database string `yaml:"database"`
}

func (c *Config) Validate() error {
	return nil
}

func connect(config *Config) (*sql.DB, error) {
	dsn := fmt.Sprintf("%s:%s@tcp(%s:%d)/%s?parseTime=true", config.User, config.Password, config.Host, config.Port, config.Database)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, fmt.Errorf("couldn't open database: %w", err)
	}
	db.SetMaxOpenConns(128)
	return db, nil
}

func Creator(ctx context.Context, configUntyped plugins.ConfigDecoder) (physical.Database, error) {
	var cfg Config
	if err := configUntyped.Decode(&cfg); err != nil {
		return nil, err
	}
	return &Database{
		Config:  &cfg,
		Verbose: os.Getenv("OCTOSQL_MYSQL_VERBOSE") == "1",
	}, nil
}

type Database struct {
	Config  *Config
	Verbose bool
}

func (d *Database) ListTables(ctx context.Context) ([]string, error) {
	panic("implement me")
}

func (d *Database) GetTable(ctx context.Context, name string, options map[string]string) (physical.DatasourceImplementation, physical.Schema, error) {
	db, err := connect(d.Config)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't connect to database: %w", err)
	}

	rows, err := db.QueryContext(ctx, "SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = ? AND table_schema = ? ORDER BY ordinal_position", name, d.Config.Database)
	if err != nil {
		return nil, physical.Schema{}, fmt.Errorf("couldn't describe table: %w", err)
	}

	var descriptions [][]string
	for rows.Next() {
		desc := make([]string, 3)
		if err := rows.Scan(&desc[0], &desc[1], &desc[2]); err != nil {
			return nil, physical.Schema{}, fmt.Errorf("couldn't scan table description: %w", err)
		}
		descriptions = append(descriptions, desc)
	}
	if len(descriptions) == 0 {
		return nil, physical.Schema{}, fmt.Errorf("table %s does not exist", name)
	}
	if d.Verbose {
		log.Printf("Table field descriptions (SELECT column_name, data_type, is_nullable FROM information_schema.columns WHERE table_name = '%s' ORDER BY ordinal_position): %+v", name, descriptions)
	}

	fields := make([]physical.SchemaField, 0, len(descriptions))
	for i := range descriptions {
		t, ok := getOctoSQLType(descriptions[i][1])
		if !ok {
			continue
		}
		if descriptions[i][2] == "YES" {
			t = octosql.TypeSum(t, octosql.Null)
		}
		fields = append(fields, physical.SchemaField{
			Name: descriptions[i][0],
			Type: t,
		})
	}
	if d.Verbose {
		log.Printf("Inferred schema (%s): %+v", name, fields)
	}

	return &impl{
			config:  d.Config,
			table:   name,
			verbose: d.Verbose,
		},
		physical.Schema{
			Fields:    fields,
			TimeField: -1,
		},
		nil
}

func getOctoSQLType(typename string) (octosql.Type, bool) {
	if strings.HasPrefix(typename, "_") {
		elementType, ok := getOctoSQLType(typename[1:])
		if !ok {
			return octosql.Type{}, false
		}

		return octosql.Type{
			TypeID: octosql.TypeIDList,
			List: struct {
				Element *octosql.Type
			}{Element: &elementType},
		}, true
	}

	switch typename {
	case "int", "smallint":
		return octosql.Int, true
	case "char", "varchar", "text":
		return octosql.String, true
	case "real", "numeric", "float":
		return octosql.Float, true
	case "bool", "boolean":
		return octosql.Boolean, true
	case "datetime", "date", "timestamp":
		return octosql.Time, true
	default:
		log.Printf("unsupported mysql field type '%s' - ignoring column", typename)
		return octosql.Null, false
	}
}

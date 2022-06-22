package internal

import (
	"context"
	"database/sql"
	"fmt"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"strings"
	"time"
)

type VitessTablet struct {
	Cell                 string
	Keyspace             string
	Shard                string
	TabletType           string
	State                string
	Alias                string
	Hostname             string
	PrimaryTermStartTime string
}

type PlanetScaleEdgeMysqlAccess interface {
	PingContext(context.Context, PlanetScaleSource) error
	GetTableNames(context.Context, PlanetScaleSource) ([]string, error)
	GetTableSchema(context.Context, PlanetScaleSource, string) (map[string]StreamProperty, error)
	GetTablePrimaryKeys(context.Context, PlanetScaleSource, string) ([]string, error)
	Close() error
}

func NewMySQL(psc *PlanetScaleSource) (PlanetScaleEdgeMysqlAccess, error) {
	db, err := sql.Open("mysql", psc.DSN(psdbconnect.TabletType_primary))
	if err != nil {
		return nil, err
	}

	return planetScaleEdgeMySQLAccess{
		db: db,
	}, nil
}

type planetScaleEdgeMySQLAccess struct {
	db *sql.DB
}

func (p planetScaleEdgeMySQLAccess) Close() error {
	return p.db.Close()
}

func (p planetScaleEdgeMySQLAccess) PingContext(ctx context.Context, psc PlanetScaleSource) error {

	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	return p.db.PingContext(ctx)
}

func (p planetScaleEdgeMySQLAccess) GetTableNames(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	var tables []string

	tableNamesQR, err := p.db.Query(fmt.Sprintf("show tables from `%s`;", psc.Database))
	if err != nil {
		return tables, errors.Wrap(err, "Unable to query database for schema")
	}

	for tableNamesQR.Next() {
		var name string
		if err = tableNamesQR.Scan(&name); err != nil {
			return tables, errors.Wrap(err, "unable to get table names")
		}

		tables = append(tables, name)
	}

	if err := tableNamesQR.Err(); err != nil {
		return tables, errors.Wrap(err, "unable to iterate table rows")
	}

	return tables, nil
}

func (p planetScaleEdgeMySQLAccess) GetTableSchema(ctx context.Context, psc PlanetScaleSource, tableName string) (map[string]StreamProperty, error) {
	properties := map[string]StreamProperty{}

	columnNamesQR, err := p.db.QueryContext(
		ctx,
		"select column_name, column_type from information_schema.columns where table_name=? AND table_schema=?;",
		tableName, psc.Database,
	)
	if err != nil {
		return properties, errors.Wrapf(err, "Unable to get column names & types for table %v", tableName)
	}

	for columnNamesQR.Next() {
		var (
			name       string
			columnType string
		)
		if err = columnNamesQR.Scan(&name, &columnType); err != nil {
			return properties, errors.Wrapf(err, "Unable to scan row for column names & types of table %v", tableName)
		}

		properties[name] = getJsonSchemaType(columnType)
	}

	if err := columnNamesQR.Err(); err != nil {
		return properties, errors.Wrapf(err, "unable to iterate columns for table %s", tableName)
	}

	return properties, nil
}

func (p planetScaleEdgeMySQLAccess) GetTablePrimaryKeys(ctx context.Context, psc PlanetScaleSource, tableName string) ([]string, error) {
	var primaryKeys []string

	primaryKeysQR, err := p.db.QueryContext(
		ctx,
		"select column_name from information_schema.columns where table_schema=? AND table_name=? AND column_key='PRI';",
		psc.Database, tableName,
	)

	if err != nil {
		return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
	}

	for primaryKeysQR.Next() {
		var name string
		if err = primaryKeysQR.Scan(&name); err != nil {
			return primaryKeys, errors.Wrapf(err, "Unable to scan row for primary keys of table %v", tableName)
		}

		primaryKeys = append(primaryKeys, name)
	}

	if err := primaryKeysQR.Err(); err != nil {
		return primaryKeys, errors.Wrapf(err, "unable to iterate primary keys for table %s", tableName)
	}

	return primaryKeys, nil
}

// Convert columnType to Singer type.
func getJsonSchemaType(mysqlType string) StreamProperty {
	if strings.HasPrefix(mysqlType, "int") {
		return StreamProperty{Types: []string{
			"null",
			"integer",
		}}
	}

	if strings.HasPrefix(mysqlType, "bigint") {
		return StreamProperty{Types: []string{"null", "string"}, CustomFormat: "big_integer"}
	}

	if strings.HasPrefix(mysqlType, "datetime") {
		return StreamProperty{Types: []string{"null", "string"}, CustomFormat: "timestamp_with_timezone"}
	}

	switch mysqlType {
	case "tinyint(1)":
		return StreamProperty{Types: []string{"null", "boolean"}}
	case "date":
		return StreamProperty{Types: []string{"null", "date"}}
	case "datetime":
		return StreamProperty{Types: []string{"null", "date"}}
	default:
		return StreamProperty{Types: []string{"null", "string"}}
	}
}

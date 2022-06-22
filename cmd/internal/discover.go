package internal

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
)

func Discover(ctx context.Context, source PlanetScaleSource) (Catalog, error) {
	var c Catalog
	mysql, err := NewMySQL(&source)
	if err != nil {
		return c, errors.Wrap(err, "unable to open mysql connection to PlanetScale Database")
	}
	defer mysql.Close()

	tableNames, err := mysql.GetTableNames(ctx, source)
	if err != nil {
		return c, errors.Wrap(err, "unable to retrieve table names")
	}

	for _, name := range tableNames {
		table := Stream{
			Name:      name,
			ID:        fmt.Sprintf("%s:%s", source.Database, name),
			TableName: name,
		}

		tableSchema, err := mysql.GetTableSchema(ctx, source, name)
		if err != nil {
			return c, errors.Wrapf(err, "unable to retrieve schema for table : %v , failed with : %q", name, err)
		}

		table.Schema = StreamSchema{
			Type:       []string{"null", "object"},
			Properties: tableSchema,
		}
		keyProperties, err := mysql.GetTablePrimaryKeys(ctx, source, name)
		if err != nil {
			return c, errors.Wrapf(err, "unable to retrieve primary keys for table : %v , failed with : %q", name, err)
		}
		table.KeyProperties = keyProperties
		table.CursorProperties = keyProperties
		table.GenerateMetadata()

		c.Streams = append(c.Streams, table)
	}

	return c, nil
}

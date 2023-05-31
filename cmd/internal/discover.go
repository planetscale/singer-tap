package internal

import (
	"context"
	"fmt"
	"strings"

	"github.com/pkg/errors"
)

type DiscoverSettings struct {
	AutoSelectTables      bool
	ExcludedTables        []string
	UseIncrementalSync    bool
	TreatTinyIntAsBoolean bool
}

func Discover(ctx context.Context, source PlanetScaleSource, mysql PlanetScaleEdgeMysqlAccess, settings DiscoverSettings) (Catalog, error) {
	var c Catalog
	if err := mysql.PingContext(ctx, source); err != nil {
		return c, errors.Wrap(err, "unable to access PlanetScale Database")
	}

	tableNames, err := mysql.GetTableNames(ctx, source)
	if err != nil {
		return c, errors.Wrap(err, "unable to retrieve table names")
	}

	excludedTables := strings.Join(settings.ExcludedTables, " ")

	for _, name := range tableNames {
		if len(excludedTables) > 0 && strings.Contains(excludedTables, name) {
			continue
		}

		table := Stream{
			Name:      name,
			ID:        fmt.Sprintf("%s:%s", source.Database, name),
			TableName: name,
		}

		tableSchema, err := mysql.GetTableSchema(ctx, source, name, settings.TreatTinyIntAsBoolean)
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
		table.GenerateMetadata(keyProperties, settings.AutoSelectTables, settings.UseIncrementalSync)

		c.Streams = append(c.Streams, table)
	}

	return c, nil
}

package internal

import (
	"context"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
)

func Sync(ctx context.Context, logger Logger, source PlanetScaleSource, catalog Catalog, state State) error {
	s, err := filterSchema(catalog)
	if err != nil {
		return errors.Wrap(err, "unable to filter schema")
	}

	mysql, err := NewMySQL(&source)
	if err != nil {
		return errors.Wrap(err, "unable to create mysql connection")
	}

	ped := PlanetScaleEdgeDatabase{
		Mysql:  mysql,
		Logger: logger,
	}
	shards, err := mysql.GetVitessShards(ctx, source)
	if err != nil {
		return err
	}

	initialState, err := source.GetInitialState(source.Database, shards)
	if err != nil {
		return err
	}

	for _, stream := range s.Streams {
		logger.StreamSchema(stream)

		// TODO : implement incremental sync
		if stream.IncrementalSyncRequested() {
			return errors.New("Incrmental sync is not yet supported.")
		}

		for shard := range initialState.Shards {
			ped.Read(ctx, source, stream, &psdbconnect.TableCursor{
				Shard:    shard,
				Keyspace: source.Database,
			})
		}
	}
	return nil
}

// filterSchema returns only the selected streams from a given catalog
func filterSchema(catalog Catalog) (Catalog, error) {
	filteredCatalog := Catalog{
		Type: "CATALOG",
	}
	for _, stream := range catalog.Streams {

		tableMetadata, err := stream.GetTableMetadata()
		if err != nil {
			return filteredCatalog, err
		}

		// if stream is selected.
		if tableMetadata.Metadata.Selected {
			fstream := stream
			// empty out the properties of this stream
			// add back only the properties that are selected.
			fstream.Schema.Properties = make(map[string]StreamProperty)
			propertyMetadataMap := stream.Metadata.GetPropertyMap()
			for name, prop := range stream.Schema.Properties {
				// if field was selected
				if propertyMetadataMap[name].Metadata.Selected {
					fstream.Schema.Properties[name] = prop
				}
			}
			filteredCatalog.Streams = append(filteredCatalog.Streams, fstream)
		}

	}
	return filteredCatalog, nil
}

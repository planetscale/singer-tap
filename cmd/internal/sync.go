package internal

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
)

func Sync(ctx context.Context, logger Logger, source PlanetScaleSource, catalog Catalog, state State) error {
	filteredSchema, err := filterSchema(catalog)
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

	emptyState := generateEmptyState(source, filteredSchema, shards)
	if emptyState == nil {
		return errors.New("unable to generate empty state")
	}

	for _, stream := range filteredSchema.Streams {
		logger.StreamSchema(stream)
		var streamShardStates map[string]*SerializedCursor
		if stream.IncrementalSyncRequested() {
			logger.Info(fmt.Sprintf("Stream %q will be synced incrementally", stream.Name))
			streamShardStates = state.Streams[stream.Name].Shards
		} else {
			streamShardStates = emptyState.Streams[stream.Name].Shards
		}

		for shard, cursor := range streamShardStates {
			logger.Info(fmt.Sprintf("syncing rows from stream %q from shard %q", stream.Name, shard))
			tc, err := cursor.SerializedCursorToTableCursor()
			if err != nil {
				return err
			}

			logger.Info(fmt.Sprintf("stream's known position is %q", tc.Position))

			state.Streams[stream.Name].Shards[shard], err = ped.Read(ctx, source, stream, tc)
			if err != nil {
				return err
			}
		}
	}

	return logger.State(state)
}

func generateEmptyState(source PlanetScaleSource, catalog Catalog, shards []string) *State {
	var s State
	initialState, err := source.GetInitialState(source.Database, shards)
	if err != nil {
		return nil
	}

	s = State{
		Streams: map[string]ShardStates{},
	}

	for _, stream := range catalog.Streams {
		s.Streams[stream.Name] = initialState
	}

	return &s
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

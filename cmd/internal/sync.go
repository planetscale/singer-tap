package internal

import (
	"context"
	"fmt"
	"strings"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"vitess.io/vitess/go/sqltypes"

	"github.com/pkg/errors"
)

func Sync(ctx context.Context, mysqlDatabase PlanetScaleEdgeMysqlAccess, edgeDatabase PlanetScaleDatabase, logger Logger, source PlanetScaleSource, catalog Catalog, state *State, recordWriter RecordWriter, tabletType psdbconnect.TabletType) error {
	// The schema as its stored by Stitch needs to be filtered before it can be synced by the tap.
	filteredSchema, err := filterSchema(catalog)
	if err != nil {
		return errors.Wrap(err, "unable to filter schema")
	}

	// get the list of vitess shards so we can generate the empty state for a sync operation.
	shards, err := mysqlDatabase.GetVitessShards(ctx, source)
	if err != nil {
		return err
	}

	tablets, err := mysqlDatabase.GetVitessTablets(ctx, source)
	if err != nil {
		return err
	}

	cells, err := findSuitableCells(tabletType, tablets)
	if err != nil {
		return err
	}

	// not all streams in a schema might need to be incrementally synced,
	// generate an empty state that starts the sync at the beginning
	// for any streams that require a FULL_TABLE sync.
	beginningState := generateEmptyState(source, filteredSchema, shards)
	if beginningState == nil {
		return errors.New("unable to generate empty state")
	}

	// if there is existing state, ensure that all selected tables
	// have valid state.
	if state != nil && len(state.Streams) > 0 {
		// copy over empty state for all tables that aren't in the existing state.
		for key, value := range beginningState.Streams {
			if _, ok := state.Streams[key]; !ok {
				state.Streams[key] = value
			}
		}
	} else {
		// if there is no last known state, start from the beginning.
		state = beginningState
	}
	// For every stream processed by this loop, across all selected shards, we output the following messages
	// ONE message of type SCHEMA with the schema of the stream that is being synced.
	// MANY messages of type RECORD, one per row in the database for this stream.
	// ONE-MANY messages of type STATE, which record the current state of the stream.
	for _, stream := range filteredSchema.Streams {
		// The first message before outputting any records for a stream
		// should always be a SCHEMA message with the schema of the stream.
		logger.StreamSchema(stream)
		var streamShardStates map[string]*SerializedCursor
		if stream.IncrementalSyncRequested() {
			logger.Info(fmt.Sprintf("Stream %q will be synced incrementally", stream.Name))
			// Use the last known state of the stream if it exists.
			if existingState, ok := state.Streams[stream.Name]; ok {
				streamShardStates = existingState.Shards
			} else {
				// selected Stream does not have any previously recorded state,
				// start from the beginning.
				streamShardStates = beginningState.Streams[stream.Name].Shards
				// copy the beginning cursor to the state
				// so that we can update it when this stream syncs.
				state.Streams[stream.Name] = beginningState.Streams[stream.Name]
			}
		} else {
			streamShardStates = beginningState.Streams[stream.Name].Shards
		}

		for shard, cursor := range streamShardStates {
			tc, err := cursor.SerializedCursorToTableCursor()
			if err != nil {
				return err
			}

			logger.Info(fmt.Sprintf("syncing rows from stream %q from shard %q at position [%v]", stream.Name, shard, tc.Position))
			if len(tc.Position) > 0 {
				logger.Info(fmt.Sprintf("stream's known position is %q", tc.Position))
			}

			needsFlush := true
			onResult := func(sqlResult *sqltypes.Result) error {
				needsFlush = true
				return printQueryResult(sqlResult, stream, recordWriter)
			}

			onCursor := func(cursor *psdbconnect.TableCursor) error {
				sc, err := TableCursorToSerializedCursor(cursor)
				if err != nil {
					return err
				}
				state.Streams[stream.Name].Shards[shard] = sc

				if needsFlush {
					return recordWriter.Flush(stream)
				}

				return nil
			}

			newCursor, err := edgeDatabase.Read(ctx, ReadParams{
				Source:            source,
				Table:             stream,
				LastKnownPosition: tc,
				Columns:           stream.Metadata.GetSelectedProperties(),
				OnCursor:          onCursor,
				OnResult:          onResult,
				TabletType:        tabletType,
				Cells:             cells,
			})
			if err != nil {
				return err
			}

			if err := recordWriter.Flush(stream); err != nil {
				return errors.Wrap(err, "unable to flush records")
			}

			if newCursor == nil {
				return errors.New("should return valid cursor, got nil")
			}

			state.Streams[stream.Name].Shards[shard] = newCursor

			if err := recordWriter.State(*state); err != nil {
				return errors.Wrap(err, "unable to serialize state")
			}
		}
	}

	return recordWriter.State(*state)
}

func printQueryResult(qr *sqltypes.Result, s Stream, recordWriter RecordWriter) error {
	data := QueryResultToRecords(qr)
	for _, datum := range data {
		subset := map[string]interface{}{}
		for _, selectedProperty := range s.Metadata.GetSelectedProperties() {
			streamProperty := s.Schema.Properties[selectedProperty]
			subset[selectedProperty] = datum[selectedProperty]
			val, err := Convert(streamProperty, datum[selectedProperty].(sqltypes.Value))
			if err != nil {
				return errors.Wrapf(err, "unable to serialize [%v] as [%v]", datum[selectedProperty], s.Schema.Properties[selectedProperty].Types)
			}
			subset[selectedProperty] = val
		}

		record := NewRecord()
		record.Stream = s.Name
		record.Data = subset
		if err := recordWriter.Record(record, s); err != nil {
			return err
		}
	}

	return nil
}

func generateEmptyState(source PlanetScaleSource, catalog Catalog, shards []string) *State {
	s := State{
		Streams: map[string]ShardStates{},
	}

	for _, stream := range catalog.Streams {
		initialState, err := source.GetInitialState(source.Database, shards)
		if err != nil {
			return nil
		}
		s.Streams[stream.Name] = initialState
	}

	return &s
}

// filterSchema returns only the selected streams from a given catalog
func filterSchema(catalog Catalog) (Catalog, error) {
	filteredCatalog := Catalog{}
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
			fstream.Metadata = MetadataCollection{}
			propertyMetadataMap := stream.Metadata.GetPropertyMap()
			for name, prop := range stream.Schema.Properties {
				// if field was selected
				if propertyMetadataMap[name].Metadata.Selected || propertyMetadataMap[name].Metadata.Inclusion == "automatic" {
					fstream.Schema.Properties[name] = prop
					fstream.Metadata = append(fstream.Metadata, propertyMetadataMap[name])
				}

				// if this is a key property, it will always be selected.
				for _, keyProp := range tableMetadata.Metadata.TableKeyProperties {
					if name == keyProp {
						fstream.Schema.Properties[name] = prop
						fstream.Metadata = append(fstream.Metadata, propertyMetadataMap[name])
					}
				}
			}

			// copy over the metadata item that refers to the Table.
			tm, err := stream.GetTableMetadata()
			if err != nil {
				return filteredCatalog, err
			}
			fstream.Metadata = append(fstream.Metadata, *tm)
			filteredCatalog.Streams = append(filteredCatalog.Streams, fstream)
		}

	}
	return filteredCatalog, nil
}

func findSuitableCells(tabletType psdbconnect.TabletType, tablets []VitessTablet) ([]string, error) {
	s := "PRIMARY"
	if tabletType == psdbconnect.TabletType_replica {
		s = "REPLICA"
	} else if tabletType == psdbconnect.TabletType_read_only {
		s = "RDONLY"
	}
	var cells []string
	for _, tablet := range tablets {
		if strings.ToUpper(tablet.TabletType) == s {
			cells = append(cells, tablet.Cell)
		}
	}

	if len(cells) == 0 {
		return nil, fmt.Errorf("unable to find any suitable tablets of type %q", s)
	}
	return cells, nil
}

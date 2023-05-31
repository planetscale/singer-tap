package internal

import (
	"context"
	"testing"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
)

func TestSync_CanFilterSchema(t *testing.T) {
	tma := getTestMysqlAccess()
	var streamsRead []string
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			streamsRead = append(streamsRead, s.Name)
			return TableCursorToSerializedCursor(tc)
		},
	}
	logger := &testSingerLogger{}
	source := PlanetScaleSource{}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:   true,
							BreadCrumb: []string{},
						},
					},
				},
			},
			{
				Name:      "departments",
				TableName: "departments",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:   false,
							BreadCrumb: []string{},
						},
					},
				},
			},
		},
	}
	err := Sync(context.Background(), tma, ped, logger, source, catalog, nil, logger, 0)
	assert.Nil(t, err)
	assert.Equal(t, []string{"employees"}, streamsRead, "should filter schema down to only selected tables.")
}

func TestSync_CanStartFromEmptyState(t *testing.T) {
	tma := getTestMysqlAccess()
	var cursor *psdbconnect.TableCursor
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			assert.Empty(t, tc.Position, "start position should be empty")
			cursor = tc
			cursor.Position = "I-HAVE-MOVED"
			return TableCursorToSerializedCursor(tc)
		},
	}
	logger := &testSingerLogger{}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							ReplicationMethod: "INCREMENTAL",
							Selected:          true,
							BreadCrumb:        []string{},
						},
					},
				},
			},
			{
				Name:      "customers",
				TableName: "customers",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							ReplicationMethod: "INCREMENTAL",
							Selected:          true,
							BreadCrumb:        []string{},
						},
					},
				},
			},
		},
	}

	err := Sync(context.Background(), tma, ped, logger, source, catalog, nil, logger, 0)
	assert.Nil(t, err)
	assert.Equal(t, source.Database, cursor.Keyspace)
	assert.Equal(t, "-", cursor.Shard)
}

func TestSync_PrintsStreamSchema(t *testing.T) {
	tma := getTestMysqlAccess()
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			return TableCursorToSerializedCursor(tc)
		},
	}
	logger := &testSingerLogger{}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Schema: StreamSchema{
					Properties: map[string]StreamProperty{
						"emp_no":     {Types: []string{"null", "string"}},
						"first_name": {Types: []string{"null", "string"}},
						"last_name":  {Types: []string{"null", "string"}},
					},
				},
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:   true,
							BreadCrumb: []string{},
						},
					},
					{
						Metadata: NodeMetadata{
							Inclusion:  "automatic",
							BreadCrumb: []string{"properties", "emp_no"},
						},
					},
					{
						Metadata: NodeMetadata{
							Selected:   false,
							BreadCrumb: []string{"properties", "first_name"},
						},
					},
					{
						Metadata: NodeMetadata{
							Selected:   true,
							BreadCrumb: []string{"properties", "last_name"},
						},
					},
				},
			},
		},
	}

	err := Sync(context.Background(), tma, ped, logger, source, catalog, nil, logger, 0)
	assert.Nil(t, err)
	printedSchema := logger.streamSchemas["employees"]
	assert.NotNil(t, printedSchema)
	assert.NotEmpty(t, printedSchema.Properties["emp_no"])
	assert.NotEmpty(t, printedSchema.Properties["last_name"])
	assert.Empty(t, printedSchema.Properties["first_name"], "should print schema of only selected properties in type")
}

func TestSync_PrintsStreamState(t *testing.T) {
	tma := getTestMysqlAccess()
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			return TableCursorToSerializedCursor(tc)
		},
	}
	logger := &testSingerLogger{}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Schema: StreamSchema{
					Properties: map[string]StreamProperty{
						"emp_no":     {Types: []string{"null", "string"}},
						"first_name": {Types: []string{"null", "string"}},
						"last_name":  {Types: []string{"null", "string"}},
					},
				},
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:   true,
							BreadCrumb: []string{},
						},
					},
					{
						Metadata: NodeMetadata{
							Inclusion:  "automatic",
							BreadCrumb: []string{"properties", "emp_no"},
						},
					},
					{
						Metadata: NodeMetadata{
							Selected:   false,
							BreadCrumb: []string{"properties", "first_name"},
						},
					},
					{
						Metadata: NodeMetadata{
							Selected:   true,
							BreadCrumb: []string{"properties", "last_name"},
						},
					},
				},
			},
		},
	}

	err := Sync(context.Background(), tma, ped, logger, source, catalog, nil, logger, 0)
	assert.Nil(t, err)
	assert.Len(t, logger.state, 2)
	lastState := logger.state[1]
	assert.NotNil(t, lastState)
	assert.NotEmpty(t, lastState)
}

func TestSync_UsesStateIfIncrementalSyncRequested(t *testing.T) {
	tma := getTestMysqlAccess()
	var cursor *psdbconnect.TableCursor
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			cursor = tc
			return TableCursorToSerializedCursor(tc)
		},
	}
	logger := &testSingerLogger{}
	sc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "sync-test",
		Position: "i-know-what-you-synced-last-summer",
	})
	assert.Nil(t, err)
	lastKnownState := State{
		Streams: map[string]ShardStates{
			"employees": {
				Shards: map[string]*SerializedCursor{
					"-": sc,
				},
			},
		},
	}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:          true,
							ReplicationMethod: "INCREMENTAL",
							BreadCrumb:        []string{},
						},
					},
				},
			},
		},
	}

	err = Sync(context.Background(), tma, ped, logger, source, catalog, &lastKnownState, logger, 0)
	assert.Nil(t, err)
	assert.Equal(t, source.Database, cursor.Keyspace)
	assert.Equal(t, "-", cursor.Shard)
	assert.Equal(t, "i-know-what-you-synced-last-summer", cursor.Position)
	assert.Equal(t, `Stream "employees" will be synced incrementally`, logger.logMessages[0])
}

func TestSync_UseOrCreateState(t *testing.T) {
	tma := getTestMysqlAccess()
	var cursor *psdbconnect.TableCursor
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			if s.Name == "employees" {
				cursor = tc
			}
			return TableCursorToSerializedCursor(tc)
		},
	}
	logger := &testSingerLogger{}
	sc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "sync-test",
		Position: "i-know-what-you-synced-last-summer",
	})
	assert.Nil(t, err)
	lastKnownState := State{
		Streams: map[string]ShardStates{
			"employees": {
				Shards: map[string]*SerializedCursor{
					"-": sc,
				},
			},
		},
	}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:          true,
							ReplicationMethod: "INCREMENTAL",
							BreadCrumb:        []string{},
						},
					},
				},
			},
			{
				Name:      "customers",
				TableName: "customers",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:   true,
							BreadCrumb: []string{},
						},
					},
				},
			},
		},
	}

	err = Sync(context.Background(), tma, ped, logger, source, catalog, &lastKnownState, logger, 0)
	assert.Nil(t, err)
	assert.Equal(t, source.Database, cursor.Keyspace)
	assert.Equal(t, "-", cursor.Shard)
	assert.Equal(t, "i-know-what-you-synced-last-summer", cursor.Position)
	assert.Equal(t, `Stream "employees" will be synced incrementally`, logger.logMessages[0])
}

func TestSync_PrintsOldStateIfNoNewStateFound(t *testing.T) {
	tma := getTestMysqlAccess()
	var cursor *psdbconnect.TableCursor

	sc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "sync-test",
		Position: "i-know-what-you-synced-last-summer",
	})

	assert.Nil(t, err)
	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			cursor = tc
			return sc, nil
		},
	}
	logger := &testSingerLogger{}
	lastKnownState := State{
		Streams: map[string]ShardStates{
			"employees": {
				Shards: map[string]*SerializedCursor{
					"-": sc,
				},
			},
		},
	}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:          true,
							ReplicationMethod: "INCREMENTAL",
							BreadCrumb:        []string{},
						},
					},
				},
			},
		},
	}

	err = Sync(context.Background(), tma, ped, logger, source, catalog, &lastKnownState, logger, 0)
	assert.Nil(t, err)
	assert.Equal(t, source.Database, cursor.Keyspace)
	assert.Equal(t, "-", cursor.Shard)
	assert.Equal(t, "i-know-what-you-synced-last-summer", cursor.Position)
	assert.Len(t, logger.state, 2)
	lastState := logger.state[1]
	assert.NotNil(t, lastState)
	assert.NotEmpty(t, lastState)

	lastSc, err := TableCursorToSerializedCursor(cursor)
	assert.Nil(t, err)
	assert.Equal(t, lastSc, lastState.Streams["employees"].Shards["-"])
}

func TestSync_PrintsNewStateIfFound(t *testing.T) {
	tma := getTestMysqlAccess()
	var cursor *psdbconnect.TableCursor

	sc, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "sync-test",
		Position: "i-know-what-you-synced-last-summer",
	})

	assert.Nil(t, err)
	newSC, err := TableCursorToSerializedCursor(&psdbconnect.TableCursor{
		Shard:    "-",
		Keyspace: "sync-test",
		Position: "i-STILL-know-what-you-synced-last-summer",
	})
	assert.Nil(t, err)

	ped := &testPlanetScaleEdgeDatabase{
		ReadFn: func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor) (*SerializedCursor, error) {
			cursor = tc
			return newSC, nil
		},
	}
	logger := &testSingerLogger{}
	lastKnownState := State{
		Streams: map[string]ShardStates{
			"employees": {
				Shards: map[string]*SerializedCursor{
					"-": sc,
				},
			},
		},
	}
	source := PlanetScaleSource{
		Database: "sync-test",
	}
	catalog := Catalog{
		Streams: []Stream{
			{
				Name:      "employees",
				TableName: "employees",
				Metadata: MetadataCollection{
					{
						Metadata: NodeMetadata{
							Selected:          true,
							ReplicationMethod: "INCREMENTAL",
							BreadCrumb:        []string{},
						},
					},
				},
			},
		},
	}

	err = Sync(context.Background(), tma, ped, logger, source, catalog, &lastKnownState, logger, 0)
	assert.Nil(t, err)
	assert.Equal(t, source.Database, cursor.Keyspace)
	assert.Equal(t, "-", cursor.Shard)
	assert.Equal(t, "i-know-what-you-synced-last-summer", cursor.Position)
	assert.Len(t, logger.state, 2)
	lastState := logger.state[1]
	assert.NotNil(t, lastState)
	assert.NotEmpty(t, lastState)

	assert.Equal(t, newSC, lastState.Streams["employees"].Shards["-"])
}

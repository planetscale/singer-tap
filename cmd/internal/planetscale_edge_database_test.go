package internal

import (
	"bytes"
	"context"
	"fmt"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"testing"
	"vitess.io/vitess/go/sqltypes"
	"vitess.io/vitess/go/vt/proto/query"
)

func TestRead_CanPeekBeforeRead(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger("test", b, b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{
				Cursor: tc,
			},
			{
				Cursor: tc,
			},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	cs := Stream{
		Name: "stream",
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanEarlyExitIfNoNewVGtidInPeek(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger("test", b, b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{}
	cs := Stream{
		Name: "stream",
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanPickPrimaryForShardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger("test", b, b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "40-80",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := Stream{
		Name: "stream",
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestDiscover_CanPickRightSingerType(t *testing.T) {
	var tests = []struct {
		MysqlType      string
		JSONSchemaType string
		SingerType     string
	}{
		{
			MysqlType:      "int(32)",
			JSONSchemaType: "integer",
			SingerType:     "",
		},
		{
			MysqlType:      "tinyint(1)",
			JSONSchemaType: "boolean",
			SingerType:     "",
		},
		{
			MysqlType:      "bigint(16)",
			JSONSchemaType: "string",
			SingerType:     "big_integer",
		},
		{
			MysqlType:      "bigint unsigned",
			JSONSchemaType: "string",
			SingerType:     "big_integer",
		},
		{
			MysqlType:      "bigint zerofill",
			JSONSchemaType: "string",
			SingerType:     "big_integer",
		},
		{
			MysqlType:      "datetime",
			JSONSchemaType: "string",
			SingerType:     "timestamp_with_timezone",
		},
		{
			MysqlType:      "date",
			JSONSchemaType: "string",
			SingerType:     "date-time",
		},
		{
			MysqlType:      "text",
			JSONSchemaType: "string",
			SingerType:     "",
		},
		{
			MysqlType:      "varchar(256)",
			JSONSchemaType: "string",
			SingerType:     "",
		},
	}

	for _, typeTest := range tests {

		t.Run(fmt.Sprintf("mysql_type_%v", typeTest.MysqlType), func(t *testing.T) {
			p := getJsonSchemaType(typeTest.MysqlType)
			assert.Equal(t, typeTest.SingerType, p.CustomFormat, "wrong custom format")
			assert.Equal(t, typeTest.JSONSchemaType, p.Types[1], "wrong jsonschema type")
		})
	}
}
func TestRead_CanPickPrimaryForUnshardedKeyspaces(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger("test", b, b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{
				Cursor: tc,
			},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := Stream{
		Name: "stream",
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
	assert.False(t, tma.PingContextFnInvoked)
	assert.False(t, tma.GetVitessTabletsFnInvoked)
}

func TestRead_CanReturnOriginalCursorIfNoNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger("test", b, b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: tc},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := Stream{
		Name: "stream",
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(tc)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 1, cc.syncFnInvokedCount)
}

func TestRead_CanReturnNewCursorIfNewFound(t *testing.T) {
	tma := getTestMysqlAccess()
	b := bytes.NewBufferString("")
	ped := PlanetScaleEdgeDatabase{
		Logger: NewLogger("test", b, b),
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}
	newTC := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "I_AM_FARTHER_IN_THE_BINLOG",
		Keyspace: "connect-test",
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: newTC},
			{Cursor: newTC},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := Stream{
		Name: "stream",
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	esc, err := TableCursorToSerializedCursor(newTC)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)
}

func TestRead_CanStopAtWellKnownCursor(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testSingerLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}

	numResponses := 10
	// when the client tries to get the "current" vgtid,
	// we return the ante-penultimate element of the array.
	currentVGtidPosition := (numResponses * 3) - 4
	// this is the next vgtid that should stop the sync session.
	nextVGtidPosition := currentVGtidPosition + 1
	responses := make([]*psdbconnect.SyncResponse, 0, numResponses)
	for i := 0; i < numResponses; i++ {
		// this simulates multiple events being returned, for the same vgtid, from vstream
		for x := 0; x < 3; x++ {
			var result []*query.QueryResult
			if x == 2 {
				result = []*query.QueryResult{
					sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
						"pid|description",
						"int64|varbinary"),
						fmt.Sprintf("%v|keyboard", i+1),
						fmt.Sprintf("%v|monitor", i+2),
					)),
				}
			}

			vgtid := fmt.Sprintf("e4e20f06-e28f-11ec-8d20-8e7ac09cb64c:1-%v", i)
			responses = append(responses, &psdbconnect.SyncResponse{
				Cursor: &psdbconnect.TableCursor{
					Shard:    "-",
					Keyspace: "connect-test",
					Position: vgtid,
				},
				Result: result,
			})
		}
	}

	syncClient := &connectSyncClientMock{
		syncResponses: responses,
	}

	getCurrentVGtidClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			responses[currentVGtidPosition],
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			if in.Cursor.Position == "current" {
				return getCurrentVGtidClient, nil
			}

			return syncClient, nil
		},
	}

	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := Stream{
		Name: "customers",
	}

	sc, err := ped.Read(context.Background(), ps, cs, responses[0].Cursor)
	assert.NoError(t, err)
	// sync should start at the first vgtid
	esc, err := TableCursorToSerializedCursor(responses[nextVGtidPosition].Cursor)
	assert.NoError(t, err)
	assert.Equal(t, esc, sc)
	assert.Equal(t, 2, cc.syncFnInvokedCount)

	logLines := tal.logMessages
	assert.Equal(t, "[customers shard : -] Finished reading all rows for table [customers]", logLines[len(logLines)-1])
	records := tal.records["customers"]
	assert.Equal(t, 2*(nextVGtidPosition/3), len(records))
}

func TestRead_CanLogResults(t *testing.T) {
	tma := getTestMysqlAccess()
	tal := testSingerLogger{}
	ped := PlanetScaleEdgeDatabase{
		Logger: &tal,
		Mysql:  tma,
	}
	tc := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "THIS_IS_A_SHARD_GTID",
		Keyspace: "connect-test",
	}
	newTC := &psdbconnect.TableCursor{
		Shard:    "-",
		Position: "I_AM_FARTHER_IN_THE_BINLOG",
		Keyspace: "connect-test",
	}

	result := []*query.QueryResult{
		sqltypes.ResultToProto3(sqltypes.MakeTestResult(sqltypes.MakeTestFields(
			"pid|description",
			"int64|varbinary"),
			"1|keyboard",
			"2|monitor",
		)),
	}

	syncClient := &connectSyncClientMock{
		syncResponses: []*psdbconnect.SyncResponse{
			{Cursor: newTC, Result: result},
			{Cursor: newTC, Result: result},
		},
	}

	cc := clientConnectionMock{
		syncFn: func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
			assert.Equal(t, psdbconnect.TabletType_primary, in.TabletType)
			return syncClient, nil
		},
	}
	ped.clientFn = func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error) {
		return &cc, nil
	}
	ps := PlanetScaleSource{
		Database: "connect-test",
	}
	cs := Stream{
		Name: "products",
		Schema: StreamSchema{
			Properties: map[string]StreamProperty{
				"pid": {
					Types: []string{"null", "integer"},
				},
				"description": {
					Types: []string{"null", "string"},
				},
			},
		},
		Metadata: MetadataCollection{
			Metadata{
				Metadata: NodeMetadata{
					Selected:   true,
					BreadCrumb: []string{},
				},
			},
			Metadata{
				Metadata: NodeMetadata{
					Selected:   true,
					BreadCrumb: []string{"properties", "id"},
				},
			},
			Metadata{
				Metadata: NodeMetadata{
					Selected:   true,
					BreadCrumb: []string{"properties", "description"},
				},
			},
		},
	}
	sc, err := ped.Read(context.Background(), ps, cs, tc)
	assert.NoError(t, err)
	assert.NotNil(t, sc)
	assert.Equal(t, 2, len(tal.records["products"]))
	records := tal.records["products"]
	keyboardFound := false
	monitorFound := false
	for _, r := range records {
		id, err := r.Data["pid"].(sqltypes.Value).ToInt64()
		assert.NoError(t, err)
		if id == 1 {
			assert.False(t, keyboardFound, "should not find keyboard twice")
			keyboardFound = true
			assert.Equal(t, "keyboard", r.Data["description"].(sqltypes.Value).ToString())
		}

		if id == 2 {
			assert.False(t, monitorFound, "should not find monitor twice")
			monitorFound = true
			assert.Equal(t, "monitor", r.Data["description"].(sqltypes.Value).ToString())
		}
	}
	assert.True(t, keyboardFound)
	assert.True(t, monitorFound)
}

func getTestMysqlAccess() *mysqlAccessMock {
	tma := mysqlAccessMock{
		PingContextFn: func(ctx context.Context, source PlanetScaleSource) error {
			return nil
		},
		GetVitessTabletsFn: func(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
			return []VitessTablet{
				{
					Keyspace:   "connect-test",
					TabletType: TabletTypeToString(psdbconnect.TabletType_primary),
					State:      "SERVING",
				},
				{
					Keyspace:   "connect-test",
					TabletType: TabletTypeToString(psdbconnect.TabletType_replica),
					State:      "SERVING",
				},
			}, nil
		},
	}
	return &tma
}

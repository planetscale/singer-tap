package internal

import (
	"context"
	"database/sql"
	"io"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/grpc"
)

type testSingerLogger struct {
	logMessages   []string
	records       map[string][]Record
	state         []State
	streamSchemas map[string]StreamSchema
}

func (tal *testSingerLogger) Log(message string) {
	if tal.logMessages == nil {
		tal.logMessages = []string{}
	}
	tal.logMessages = append(tal.logMessages, message)
}

func (tal *testSingerLogger) Info(message string) {
	tal.logMessages = append(tal.logMessages, message)
}

type testPlanetScaleEdgeDatabase struct {
	CanConnectFn        func(ctx context.Context, ps PlanetScaleSource) error
	CanConnectFnInvoked bool
	ReadFn              func(ctx context.Context, ps PlanetScaleSource, s Stream, tc *psdbconnect.TableCursor, indexRows bool) (*SerializedCursor, error)
	ReadFnInvoked       bool
}

func (tpe *testPlanetScaleEdgeDatabase) CanConnect(ctx context.Context, ps PlanetScaleSource) error {
	tpe.CanConnectFnInvoked = true
	return tpe.CanConnectFn(ctx, ps)
}

func (tpe *testPlanetScaleEdgeDatabase) Read(ctx context.Context, ps PlanetScaleSource, table Stream, lastKnownPosition *psdbconnect.TableCursor, indexRows bool, columns []string, onResult OnResult, onCursor OnCursor) (*SerializedCursor, error) {
	tpe.ReadFnInvoked = true
	return tpe.ReadFn(ctx, ps, table, lastKnownPosition, indexRows)
}

func (tpe *testPlanetScaleEdgeDatabase) Close() error {
	// TODO implement me
	panic("implement me")
}

func (tal *testSingerLogger) Error(message string) {
	// TODO implement me
	panic("implement me")
}

func (tal *testSingerLogger) State(state State) error {
	tal.state = append(tal.state, state)
	return nil
}

func (tal *testSingerLogger) Schema(catalog Catalog) error {
	// TODO implement me
	panic("implement me")
}

func (tal *testSingerLogger) StreamSchema(stream Stream) error {
	if tal.streamSchemas == nil {
		tal.streamSchemas = map[string]StreamSchema{}
	}
	tal.streamSchemas[stream.Name] = stream.Schema
	return nil
}

func (tal *testSingerLogger) Record(record Record, stream Stream) error {
	if tal.records == nil {
		tal.records = make(map[string][]Record)
	}

	if tal.records[stream.Name] == nil {
		tal.records[stream.Name] = []Record{}
	}

	tal.records[stream.Name] = append(tal.records[stream.Name], record)
	return nil
}

func (tal *testSingerLogger) Flush(stream Stream) {
}

type clientConnectionMock struct {
	syncFn             func(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error)
	syncFnInvoked      bool
	syncFnInvokedCount int
}

type connectSyncClientMock struct {
	lastResponseSent int
	syncResponses    []*psdbconnect.SyncResponse
	grpc.ClientStream
}

func (x *connectSyncClientMock) Recv() (*psdbconnect.SyncResponse, error) {
	if x.lastResponseSent >= len(x.syncResponses) {
		return nil, io.EOF
	}
	x.lastResponseSent += 1
	return x.syncResponses[x.lastResponseSent-1], nil
}

func (c *clientConnectionMock) Sync(ctx context.Context, in *psdbconnect.SyncRequest, opts ...grpc.CallOption) (psdbconnect.Connect_SyncClient, error) {
	c.syncFnInvoked = true
	c.syncFnInvokedCount += 1
	return c.syncFn(ctx, in, opts...)
}

type mysqlAccessMock struct {
	PingContextFn                func(ctx context.Context, source PlanetScaleSource) error
	PingContextFnInvoked         bool
	GetVitessTabletsFn           func(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error)
	GetVitessTabletsFnInvoked    bool
	GetTableNamesFn              func(ctx context.Context, source PlanetScaleSource) ([]string, error)
	GetTableNamesFnInvoked       bool
	GetTableSchemaFn             func(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error)
	GetTableSchemaFnInvoked      bool
	GetTablePrimaryKeysFn        func(ctx context.Context, source PlanetScaleSource, s string) ([]string, error)
	GetTablePrimaryKeysFnInvoked bool
	GetVitessShardsFn            func(ctx context.Context, psc PlanetScaleSource) ([]string, error)
	GetVitessShardsFnInvoked     bool
}

func (tma *mysqlAccessMock) PingContext(ctx context.Context, source PlanetScaleSource) error {
	tma.PingContextFnInvoked = true
	return tma.PingContextFn(ctx, source)
}

func (tma *mysqlAccessMock) GetTableNames(ctx context.Context, source PlanetScaleSource) ([]string, error) {
	tma.GetTableNamesFnInvoked = true
	return tma.GetTableNamesFn(ctx, source)
}

func (tma *mysqlAccessMock) GetTableSchema(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
	tma.GetTableSchemaFnInvoked = true
	return tma.GetTableSchemaFn(ctx, source, s)
}

func (tma *mysqlAccessMock) GetTablePrimaryKeys(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
	tma.GetTablePrimaryKeysFnInvoked = true
	return tma.GetTablePrimaryKeysFn(ctx, source, s)
}

func (mysqlAccessMock) QueryContext(ctx context.Context, psc PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error) {
	// TODO implement me
	panic("implement me")
}

func (tma *mysqlAccessMock) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	tma.GetVitessTabletsFnInvoked = true
	return tma.GetVitessTabletsFn(ctx, psc)
}

func (tma *mysqlAccessMock) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	tma.GetVitessShardsFnInvoked = true
	return tma.GetVitessShardsFn(ctx, psc)
}
func (mysqlAccessMock) Close() error { return nil }

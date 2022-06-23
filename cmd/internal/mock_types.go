package internal

import (
	"context"
	"database/sql"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"google.golang.org/grpc"
	"io"
)

type testSingerLogger struct {
	logMessages []string
	records     map[string][]Record
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

func (tal *testSingerLogger) Error(message string) {
	//TODO implement me
	panic("implement me")
}

func (tal *testSingerLogger) Schema(catalog Catalog) error {
	//TODO implement me
	panic("implement me")
}

func (tal *testSingerLogger) StreamSchema(stream Stream) error {
	//TODO implement me
	panic("implement me")
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
	PingContextFn             func(ctx context.Context, source PlanetScaleSource) error
	PingContextFnInvoked      bool
	GetVitessTabletsFn        func(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error)
	GetVitessTabletsFnInvoked bool
}

func (tma *mysqlAccessMock) PingContext(ctx context.Context, source PlanetScaleSource) error {
	tma.PingContextFnInvoked = true
	return tma.PingContextFn(ctx, source)
}

func (mysqlAccessMock) GetTableNames(ctx context.Context, source PlanetScaleSource) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTableSchema(ctx context.Context, source PlanetScaleSource, s string) (map[string]StreamProperty, error) {
	//TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) GetTablePrimaryKeys(ctx context.Context, source PlanetScaleSource, s string) ([]string, error) {
	//TODO implement me
	panic("implement me")
}

func (mysqlAccessMock) QueryContext(ctx context.Context, psc PlanetScaleSource, query string, args ...interface{}) (*sql.Rows, error) {
	//TODO implement me
	panic("implement me")
}

func (tma *mysqlAccessMock) GetVitessTablets(ctx context.Context, psc PlanetScaleSource) ([]VitessTablet, error) {
	tma.GetVitessTabletsFnInvoked = true
	return tma.GetVitessTabletsFn(ctx, psc)
}

func (mysqlAccessMock) GetVitessShards(ctx context.Context, psc PlanetScaleSource) ([]string, error) {
	//TODO implement me
	panic("implement me")
}
func (mysqlAccessMock) Close() error { return nil }

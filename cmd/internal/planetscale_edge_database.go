package internal

import (
	"context"
	"fmt"
	"io"
	"strings"
	"time"

	querypb "vitess.io/vitess/go/vt/proto/query"

	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/auth"
	grpcclient "github.com/planetscale/psdb/core/pool"
	clientoptions "github.com/planetscale/psdb/core/pool/options"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"vitess.io/vitess/go/sqltypes"
	_ "vitess.io/vitess/go/vt/vtctl/grpcvtctlclient"
	_ "vitess.io/vitess/go/vt/vtgate/grpcvtgateconn"
)

type (
	OnResult func(*sqltypes.Result) error
	OnCursor func(*psdbconnect.TableCursor) error
)

type ReadParams struct {
	Source            PlanetScaleSource
	Table             Stream
	LastKnownPosition *psdbconnect.TableCursor
	Columns           []string
	OnResult          OnResult
	OnCursor          OnCursor
	TabletType        psdbconnect.TabletType
}

var binlogsPurgedMessage = "Cannot replicate because the master purged required binary logs"

// PlanetScaleDatabase is a general purpose interface
// that defines all the data access methods needed for the PlanetScale Singer Tap to function.
type PlanetScaleDatabase interface {
	CanConnect(ctx context.Context, ps PlanetScaleSource) error
	Read(ctx context.Context, params ReadParams) (*SerializedCursor, error)
	Close() error
}

func NewEdge(mysql PlanetScaleEdgeMysqlAccess, logger Logger) PlanetScaleDatabase {
	return &PlanetScaleEdgeDatabase{
		Mysql:  mysql,
		Logger: logger,
	}
}

// PlanetScaleEdgeDatabase is an implementation of the PlanetScaleDatabase interface defined above.
// It uses the mysql interface provided by PlanetScale for all schema/shard/tablet discovery and
// the grpc API for incrementally syncing rows from PlanetScale.
type PlanetScaleEdgeDatabase struct {
	Logger   Logger
	Mysql    PlanetScaleEdgeMysqlAccess
	clientFn func(ctx context.Context, ps PlanetScaleSource) (psdbconnect.ConnectClient, error)
}

func (p PlanetScaleEdgeDatabase) CanConnect(ctx context.Context, psc PlanetScaleSource) error {
	return p.Mysql.PingContext(ctx, psc)
}

func (p PlanetScaleEdgeDatabase) Close() error {
	return p.Mysql.Close()
}

// Read streams rows from a table given a starting cursor.
// 1. We will get the latest vgtid for a given table in a shard when a sync session starts.
// 2. This latest vgtid is now the stopping point for this sync session.
// 3. Ask vstream to stream from the last known vgtid
// 4. When we reach the stopping point, read all rows available at this vgtid
// 5. End the stream when (a) a vgtid newer than latest vgtid is encountered or (b) the timeout kicks in.
func (p PlanetScaleEdgeDatabase) Read(ctx context.Context, params ReadParams) (*SerializedCursor, error) {
	var (
		err                     error
		sErr                    error
		currentSerializedCursor *SerializedCursor
	)

	currentPosition := params.LastKnownPosition

	readDuration := 90 * time.Second
	preamble := fmt.Sprintf("[%v shard : %v] ", params.Table.Name, currentPosition.Shard)
	for {
		p.Logger.Info(preamble + "peeking to see if there's any new rows")
		latestCursorPosition, lcErr := p.getLatestCursorPosition(ctx, currentPosition.Shard, currentPosition.Keyspace, params.Table, params.Source, params.TabletType)
		if lcErr != nil {
			return currentSerializedCursor, errors.Wrap(err, "Unable to get latest cursor position")
		}

		// the current vgtid is the same as the last synced vgtid, no new rows.
		if latestCursorPosition == currentPosition.Position {
			p.Logger.Info(preamble + "no new rows found, exiting")
			return TableCursorToSerializedCursor(currentPosition)
		}
		p.Logger.Info(fmt.Sprintf(preamble+"syncing rows with cursor [%v]", currentPosition))
		p.Logger.Info(fmt.Sprintf(preamble+"latest database position is [%v]", latestCursorPosition))

		currentPosition, err = p.sync(ctx, currentPosition, latestCursorPosition, readDuration, params)
		if currentPosition.Position != "" {
			currentSerializedCursor, sErr = TableCursorToSerializedCursor(currentPosition)
			if sErr != nil {
				// if we failed to serialize here, we should bail.
				return currentSerializedCursor, errors.Wrap(sErr, "unable to serialize current position")
			}
		}
		if err != nil {
			if s, ok := status.FromError(err); ok {

				// if the error is unknown, it might be because the binlogs are purged, check for known error message
				if s.Code() == codes.Unknown && params.LastKnownPosition != nil {
					if strings.Contains(err.Error(), binlogsPurgedMessage) {
						p.Logger.Info("Binlogs are purged, state is stale")
						return currentSerializedCursor, fmt.Errorf("state for this sync operation [%v] is stale, please restart a full sync to get the latest state", params.LastKnownPosition.Position)
					}
				}
				// if the error is anything other than server timeout, keep going
				if s.Code() != codes.DeadlineExceeded {
					p.Logger.Info(fmt.Sprintf("%v Got error [%v], Returning with cursor :[%v] after server timeout", preamble, s.Code(), currentPosition))
					return currentSerializedCursor, nil
				} else {
					p.Logger.Info(preamble + "Continuing with cursor after server timeout")
				}
			} else if errors.Is(err, io.EOF) {
				p.Logger.Info(fmt.Sprintf("%vFinished reading all rows for table [%v]", preamble, params.Table.Name))
				return currentSerializedCursor, nil
			} else {
				p.Logger.Info(fmt.Sprintf("non-grpc error [%v]]", err))
				return currentSerializedCursor, err
			}
		}
	}
}

func (p PlanetScaleEdgeDatabase) sync(ctx context.Context, tc *psdbconnect.TableCursor, stopPosition string, readDuration time.Duration, params ReadParams) (*psdbconnect.TableCursor, error) {
	defer p.Logger.Flush(params.Table)
	ctx, cancel := context.WithTimeout(ctx, readDuration)
	defer cancel()

	var (
		err    error
		client psdbconnect.ConnectClient
	)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, params.Source.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(params.Source.Username, params.Source.Password).CallOption(),
			),
		)
		if err != nil {
			return tc, err
		}
		defer conn.Close()
		client = psdbconnect.NewConnectClient(conn)
	} else {
		client, err = p.clientFn(ctx, params.Source)
		if err != nil {
			return tc, err
		}
	}

	if tc.LastKnownPk != nil {
		filterFields(tc.LastKnownPk, params.Table)
		tc.Position = ""
	}

	sReq := &psdbconnect.SyncRequest{
		TableName:  params.Table.Name,
		Cursor:     tc,
		TabletType: params.TabletType,
		Columns:    params.Columns,
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return tc, err
	}

	// stop when we've reached the well known stop position for this sync session.
	watchForVgGtidChange := false

	for {

		res, err := c.Recv()
		if err != nil {
			return tc, err
		}

		if res.Cursor != nil {
			tc = res.Cursor
		}

		// Because of the ordering of events in a vstream
		// we receive the vgtid event first and then the rows.
		// the vgtid event might repeat, but they're ordered.
		// so we once we reach the desired stop vgtid, we stop the sync session
		// if we get a newer vgtid.
		watchForVgGtidChange = watchForVgGtidChange || tc.Position == stopPosition

		for _, result := range res.Result {
			qr := sqltypes.Proto3ToResult(result)
			for _, row := range qr.Rows {
				sqlResult := &sqltypes.Result{
					Fields: result.Fields,
				}
				sqlResult.Rows = append(sqlResult.Rows, row)
				if params.OnResult != nil {
					if err := params.OnResult(sqlResult); err != nil {
						return tc, err
					}
				}
			}
		}

		if params.OnCursor != nil && res.Cursor != nil {
			params.OnCursor(res.Cursor)
		}

		if watchForVgGtidChange && tc.Position != stopPosition {
			return tc, io.EOF
		}
	}
}

// filterFields removes all fields that are not part of the primary key of a given stream
// the `Fields` collection in the LastKnownPK QueryResult might contain _ALL_ the
// fields in the table and not just the fields that have values assigned to them.
// Because the field -> value mapping is ordinal based in Vitess,
// we can depend on the original Fields collection preserving the order.
func filterFields(lastKnownPK *querypb.QueryResult, s Stream) {
	var fields []*querypb.Field
	for _, field := range lastKnownPK.Fields {
		if contains(s.KeyProperties, field.Name) {
			fields = append(fields, field)
		}
	}
	lastKnownPK.Fields = fields
}

// contains checks if a string searchTerm is present in the list.
func contains(list []string, searchTerm string) bool {
	for _, val := range list {
		if searchTerm == val {
			return true
		}
	}

	return false
}

func (p PlanetScaleEdgeDatabase) getLatestCursorPosition(ctx context.Context, shard, keyspace string, s Stream, ps PlanetScaleSource, tabletType psdbconnect.TabletType) (string, error) {
	defer p.Logger.Flush(s)
	timeout := 45 * time.Second
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	var (
		err    error
		client psdbconnect.ConnectClient
	)

	if p.clientFn == nil {
		conn, err := grpcclient.Dial(ctx, ps.Host,
			clientoptions.WithDefaultTLSConfig(),
			clientoptions.WithCompression(true),
			clientoptions.WithConnectionPool(1),
			clientoptions.WithExtraCallOption(
				auth.NewBasicAuth(ps.Username, ps.Password).CallOption(),
			),
		)
		if err != nil {
			return "", err
		}
		defer conn.Close()
		client = psdbconnect.NewConnectClient(conn)
	} else {
		client, err = p.clientFn(ctx, ps)
		if err != nil {
			return "", err
		}
	}

	sReq := &psdbconnect.SyncRequest{
		TableName: s.Name,
		Cursor: &psdbconnect.TableCursor{
			Shard:    shard,
			Keyspace: keyspace,
			Position: "current",
		},
		TabletType: tabletType,
	}

	c, err := client.Sync(ctx, sReq)
	if err != nil {
		return "", nil
	}

	for {
		res, err := c.Recv()
		if err != nil {
			return "", err
		}

		if res.Cursor != nil {
			return res.Cursor.Position, nil
		}
	}
}

func getISOTimeStamp(value string) string {
	p, err := time.Parse("2006-01-02 15:04:05", value)
	if err != nil {
		return ""
	}
	return p.Format(time.RFC3339)
}

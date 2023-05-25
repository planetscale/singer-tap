package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"

	"github.com/pkg/errors"
	"github.com/planetscale/singer-tap/cmd/internal"
)

var (
	version            string
	commit             string
	date               string
	discoverMode       bool
	commitMode         bool
	catalogFilePath    string
	configFilePath     string
	stateFilePath      string
	autoSelect         bool
	useIncrementalSync bool
	useReplica         bool
	useReadOnly        bool
	excludedTables     string
	singerAPIURL       string
	batchSize          int
	apiToken           string
	stateDirectory     string
)

func init() {
	flag.BoolVar(&discoverMode, "discover", false, "Run this tap in discover mode")
	flag.StringVar(&configFilePath, "config", "", "path to a configuration file for this tap")
	flag.StringVar(&catalogFilePath, "catalog", "", "(sync mode only) path to a catalog file for this tap")
	flag.StringVar(&stateFilePath, "state", "", "(sync mode only) path to state file for this configuration")
	flag.BoolVar(&autoSelect, "auto-select", false, "(discover mode only) select all tables & columns in the schema")
	flag.BoolVar(&useIncrementalSync, "incremental", true, "(discover mode only) all tables & views will be synced incrementally")
	flag.StringVar(&excludedTables, "excluded-tables", "", "(discover mode only) comma separated list of tables & views to exclude.")
	flag.BoolVar(&useReplica, "use-replica", false, "(sync mode only) use a replica tablet to stream rows from PlanetScale")
	flag.BoolVar(&useReadOnly, "use-rdonly", false, "(sync mode only) use a readonly tablet to stream rows from PlanetScale")

	// variables for http commit mode
	flag.BoolVar(&commitMode, "commit", false, "(sync mode only) Run this tap in commit mode, sends rows to Stitch Import API")
	flag.StringVar(&singerAPIURL, "singer-api-url", "https://api.stitchdata.com", "(sync mode only) API Url for Singer")
	flag.IntVar(&batchSize, "batch-size", 9000, "(sync mode only) size of each batch sent to Singer")
	flag.StringVar(&apiToken, "singer-api-token", "", "(sync mode only) API Token to authenticate with Singer")
	flag.StringVar(&stateDirectory, "state-directory", "", "(sync mode only) Directory to save any received state")
}

func main() {
	flag.Parse()
	var recordWriter internal.RecordWriter
	logger := internal.NewLogger("PlanetScale Tap", os.Stdout, os.Stderr)
	if commitMode {
		if len(apiToken) == 0 {
			fmt.Println("Commit mode requires an apiToken, please provide a valid apiToken with the --api-token flag")
			os.Exit(1)
		}

		if len(stateDirectory) == 0 {
			fmt.Println("Commit mode requires a directory to store generated state files, please provide a valid path with the --state-directory flag")
			os.Exit(1)
		}

		recordWriter = internal.NewHttpRecordWriter(batchSize, singerAPIURL, apiToken, stateDirectory, logger)
	} else {
		recordWriter = logger
	}
	logger.Info(fmt.Sprintf("PlanetScale Singer Tap : version [%q], commit [%q], published on [%q]", version, commit, date))
	if useReplica && useReadOnly {
		fmt.Println("Only one of use-replica, use-rdonly can be specified, please pick one of these two modes and try again")
		os.Exit(1)
	}
	var tabletType psdbconnect.TabletType
	if useReplica {
		tabletType = psdbconnect.TabletType_replica
	} else if useReadOnly {
		tabletType = psdbconnect.TabletType_read_only
	} else {
		tabletType = psdbconnect.TabletType_primary
	}

	err := execute(discoverMode, logger, configFilePath, catalogFilePath, stateFilePath, recordWriter, tabletType)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func execute(discoverMode bool, logger internal.Logger, configFilePath, catalogFilePath, stateFilePath string, recordWriter internal.RecordWriter, tabletType psdbconnect.TabletType) error {
	var (
		sourceConfig internal.PlanetScaleSource
		catalog      internal.Catalog
		state        *internal.State
		err          error
	)

	if len(configFilePath) == 0 {
		return errors.New("Please specify path to a valid configuration file with the --config flag")
	}

	sourceConfig, err = internal.Parse(configFilePath, sourceConfig)
	if err != nil {
		return fmt.Errorf("config file contents are invalid: %q", err)
	}

	if discoverMode {
		logger.Info("running in discovery mode")
		settings := internal.DiscoverSettings{
			AutoSelectTables:   autoSelect,
			UseIncrementalSync: useIncrementalSync,
		}

		if len(excludedTables) > 0 {
			settings.ExcludedTables = strings.Split(excludedTables, ",")
		}

		return discover(context.Background(), logger, sourceConfig, settings)
	}

	if len(catalogFilePath) == 0 {
		return errors.New("Please specify path to a valid catalog file with the --catalog flag")
	}

	catalog, err = internal.Parse(catalogFilePath, catalog)
	if err != nil {
		return fmt.Errorf("catalog file contents are invalid: %q", err)
	}

	if len(stateFilePath) > 0 {
		state, err = internal.ParseSavedState(stateFilePath)
		if err != nil {
			return fmt.Errorf("state file contents are invalid: %q", err)
		}
	}

	return sync(context.Background(), logger, sourceConfig, catalog, state, recordWriter, tabletType)
}

func sync(ctx context.Context, logger internal.Logger, source internal.PlanetScaleSource, catalog internal.Catalog, state *internal.State, recordWriter internal.RecordWriter, tabletType psdbconnect.TabletType) error {
	logger.Info(fmt.Sprintf("Syncing records for PlanetScale database : %v", source.Database))
	mysql, err := internal.NewMySQL(&source)
	if err != nil {
		return errors.Wrap(err, "unable to create mysql connection")
	}
	defer mysql.Close()
	ped := internal.NewEdge(mysql, logger)

	return internal.Sync(ctx, mysql, ped, logger, source, catalog, state, recordWriter, tabletType)
}

func discover(ctx context.Context, logger internal.Logger, source internal.PlanetScaleSource, settings internal.DiscoverSettings) error {
	logger.Info(fmt.Sprintf("Discovering Schema for PlanetScale database : %v", source.Database))
	mysql, err := internal.NewMySQL(&source)
	if err != nil {
		return errors.Wrap(err, "unable to create mysql connection")
	}
	defer mysql.Close()

	catalog, err := internal.Discover(ctx, source, mysql, settings)
	if err != nil {
		return errors.Wrap(err, "unable to discover schema for PlanetScale database")
	}

	return logger.Schema(catalog)
}

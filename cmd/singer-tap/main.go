package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

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
	excludedTables     string
	indexRows          bool
	singerAPIURL       string
	batchSize          int
	bufferSize         int
	apiToken           string
	stateDirectory     string
)

func init() {
	flag.BoolVar(&discoverMode, "discover", false, "Run this tap in discover mode")
	flag.StringVar(&configFilePath, "config", "", "path to a configuration file for this tap")
	flag.StringVar(&catalogFilePath, "catalog", "", "path to a catalog file for this tap")
	flag.StringVar(&stateFilePath, "state", "", "path to state file for this configuration")
	flag.BoolVar(&autoSelect, "auto-select", false, "(discover mode only) select all tables & columns in the schema")
	flag.BoolVar(&useIncrementalSync, "incremental", false, "(discover mode only) all tables & views will be synced incrementally")
	flag.BoolVar(&indexRows, "index-rows", false, "index all rows in the output")
	flag.StringVar(&excludedTables, "excluded-tables", "", "(discover mode only) comma separated list of tables & views to exclude.")

	// variables for http commit mode
	flag.BoolVar(&commitMode, "commit", false, "Run this tap in commit mode")
	flag.StringVar(&singerAPIURL, "api-url", "https://api.stitchdata.com", "API Url for Singer")
	flag.IntVar(&batchSize, "batch-size", 9000, "size of each batch sent to Singer, default is 9000")
	flag.StringVar(&apiToken, "api-token", "", "API Token to authenticate with Singer")
	flag.StringVar(&stateDirectory, "state-directory", "state", "Directory to save any received state, default is state/")
	flag.IntVar(&bufferSize, "buffer-size", 1024, "size of the buffer used to read lines from STDIN, default is 1024")
}

func main() {
	flag.Parse()
	if commitMode {
		if len(apiToken) == 0 {
			fmt.Println("Commit mode requires an apiToken, please provide a valid apiToken with the --api-token flag")
			os.Exit(1)
		}
	}
	logger := internal.NewLogger("PlanetScale Tap", os.Stdout, os.Stderr)
	logger.Info(fmt.Sprintf("PlanetScale Singer Tap : version [%q], commit [%q], published on [%q]", version, commit, date))
	err := execute(discoverMode, logger, configFilePath, catalogFilePath, stateFilePath)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func execute(discoverMode bool, logger internal.Logger, configFilePath, catalogFilePath, stateFilePath string) error {
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

	return sync(context.Background(), logger, sourceConfig, catalog, state)
}

func sync(ctx context.Context, logger internal.Logger, source internal.PlanetScaleSource, catalog internal.Catalog, state *internal.State) error {
	logger.Info(fmt.Sprintf("Syncing records for PlanetScale database : %v", source.Database))
	mysql, err := internal.NewMySQL(&source)
	if err != nil {
		return errors.Wrap(err, "unable to create mysql connection")
	}
	defer mysql.Close()
	ped := internal.NewEdge(mysql, logger)

	return internal.Sync(ctx, mysql, ped, logger, source, catalog, state, indexRows)
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

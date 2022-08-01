package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"github.com/planetscale/singer-tap/cmd/internal"
	"io/ioutil"
	"os"
)

var (
	version         string
	commit          string
	date            string
	discoverMode    bool
	catalogFilePath string
	configFilePath  string
	stateFilePath   string
)

func init() {
	flag.BoolVar(&discoverMode, "discover", false, "Run this tap in discover mode")
	flag.StringVar(&configFilePath, "config", "", "path to a configuration file for this tap")
	flag.StringVar(&catalogFilePath, "catalog", "", "path to a catalog file for this tap")
	flag.StringVar(&stateFilePath, "state", "", "path to state file for this configuration")
}

func main() {
	flag.Parse()
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

	sourceConfig, err = parse(configFilePath, sourceConfig)
	if err != nil {
		return fmt.Errorf("config file contents are invalid: %q", err)
	}

	if discoverMode {
		logger.Info("running in discovery mode")
		return discover(context.Background(), logger, sourceConfig)
	}

	if len(catalogFilePath) == 0 {
		return errors.New("Please specify path to a valid catalog file with the --catalog flag")
	}

	catalog, err = parse(catalogFilePath, catalog)
	if err != nil {
		return fmt.Errorf("catalog file contents are invalid: %q", err)
	}

	if len(stateFilePath) > 0 {
		state, err = parse(stateFilePath, state)
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

	return internal.Sync(ctx, mysql, ped, logger, source, catalog, state)
}

func discover(ctx context.Context, logger internal.Logger, source internal.PlanetScaleSource) error {
	logger.Info(fmt.Sprintf("Discovering Schema for PlanetScale database : %v", source.Database))
	mysql, err := internal.NewMySQL(&source)
	if err != nil {
		return errors.Wrap(err, "unable to create mysql connection")
	}
	defer mysql.Close()

	catalog, err := internal.Discover(ctx, source, mysql, internal.DiscoverSettings{})
	if err != nil {
		return errors.Wrap(err, "unable to discover schema for PlanetScale database")
	}

	return logger.Schema(catalog)
}

func parse[T any](path string, obj T) (T, error) {
	b, err := ioutil.ReadFile(path)
	if err != nil {
		return obj, errors.Wrapf(err, "unable to read file at path %v", path)
	}

	if err = json.Unmarshal(b, &obj); err != nil {
		return obj, err
	}
	return obj, nil
}

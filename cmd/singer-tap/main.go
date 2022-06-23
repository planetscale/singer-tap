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
	syncMode        bool
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
	err := execute(discoverMode, logger, configFilePath, catalogFilePath, stateFilePath)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func execute(discoverMode bool, logger internal.Logger, configFilePath, catalogFilePath, stateFilePath string) error {

	var (
		sourceConfig internal.PlanetScaleSource
		err          error
	)

	if len(configFilePath) == 0 {
		return errors.New("Please specify path to a valid configuration file with the --config flag")
	}

	sourceConfig, err = parse(configFilePath, sourceConfig)
	if err != nil {
		return fmt.Errorf("config file contents are invalid: %q", err)
	}

	syncMode = !discoverMode

	if discoverMode {
		logger.Info("running in discovery mode")
		return discover(context.Background(), logger, sourceConfig)
	}

	if len(catalogFilePath) == 0 {
		return errors.New("Please specify path to a valid catalog file with the --catalog flag")
	}

	if syncMode {
		logger.Info("running in sync mode")
	}

	return errors.New("SYNC mode is not supported yet")
}

func discover(ctx context.Context, logger internal.Logger, source internal.PlanetScaleSource) error {
	logger.Info(fmt.Sprintf("Discovering Schema for PlanetScale database : %v", source.Database))
	catalog, err := internal.Discover(ctx, source)
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

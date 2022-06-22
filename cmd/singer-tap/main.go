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
	execute(discoverMode, configFilePath, catalogFilePath, stateFilePath)
}

func execute(discoverMode bool, configFilePath, catalogFilePath, stateFilePath string) {
	logger := internal.NewLogger(os.Stdout, os.Stderr)
	var (
		sourceConfig internal.PlanetScaleSource
		err          error
	)

	if len(configFilePath) == 0 {
		fmt.Println("Please specify path to a valid configuration file with the --config flag")
		os.Exit(1)
	}

	sourceConfig, err = parse(configFilePath, sourceConfig)
	if err != nil {
		logger.Log(fmt.Sprintf("config file contents are invalid: %q", err))
		os.Exit(1)
	}

	syncMode = !discoverMode

	if discoverMode {
		err := discover(context.Background(), logger, sourceConfig)
		if err != nil {
			logger.Error(err.Error())
			os.Exit(1)
		}
		return
	}

	if len(catalogFilePath) == 0 {
		fmt.Println("Please specify path to a valid catalog file with the --catalog flag")
		os.Exit(1)
	}

	if syncMode {
		fmt.Println("running in sync mode")
	}

	logger.Error("SYNC mode is not supported yet")
	os.Exit(1)
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

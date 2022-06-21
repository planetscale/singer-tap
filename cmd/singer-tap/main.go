package main

import (
	"flag"
	"fmt"
	"os"
)

var (
	syncMode       bool
	discoverMode   bool
	configFilePath string
	stateFilePath  string
)

func init() {
	flag.BoolVar(&discoverMode, "discover", false, "Run this tap in discover mode")
	flag.StringVar(&configFilePath, "config", "", "path to a configuration file for this tap")
	flag.StringVar(&stateFilePath, "state", "", "path to state file for this configuration")
}

func main() {
	flag.Parse()

	if len(configFilePath) == 0 {
		fmt.Println("Please specify path to a valid configuration file with the --config flag")
		os.Exit(1)
	}

	syncMode = !discoverMode
	if discoverMode {
		fmt.Println("running in discover mode")
	}

	if syncMode {
		fmt.Println("running in sync mode")
	}
}

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"github.com/planetscale/singer-tap/cmd/internal"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"
)

var (
	singerAPIURL   string
	batchSize      int
	apiToken       string
	stateDirectory string
)

func init() {
	flag.StringVar(&singerAPIURL, "api-url", "https://api.stitchdata.com", "API Url for Singer")
	flag.IntVar(&batchSize, "batch-size", 20, "size of each batch sent to Singer, default is 20")
	flag.StringVar(&apiToken, "api-token", "", "API Token to authenticate with Singer")
	flag.StringVar(&stateDirectory, "state-directory", "state", "Directory to save any received state, default is state/")
}

func main() {
	flag.Parse()

	logger := internal.NewLogger("HTTP Tap", os.Stdout, os.Stderr)
	err := execute(logger, singerAPIURL, batchSize, apiToken)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func execute(logger internal.Logger, apiUrl string, batchSize int, token string) error {

	if len(token) == 0 {
		return errors.New("Please specify a valid apiToken with the --api-token flag")
	}

	scanner := bufio.NewScanner(os.Stdin)

	var (
		stream *internal.Stream
	)

	batchWriter := internal.NewBatchWriter(batchSize, logger, apiUrl, apiToken)

	for scanner.Scan() {
		s, r, err := parseInput(scanner.Text(), logger)
		if err != nil {
			return errors.Wrap(err, "unable to process output from STDIN")
		}

		if s != nil {
			if s != stream && stream != nil {
				// The schema we're writing out has changed, flush the messages so far.
				if err := batchWriter.Flush(stream); err != nil {
					return err
				}
			}

			// we retain the catalog so we can build a BatchMessage
			stream = s
		}

		if r != nil {
			if err := batchWriter.Send(r, stream); err != nil {
				return err
			}
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	return batchWriter.Flush(stream)
}

func parseInput(input string, logger internal.Logger) (*internal.Stream, *internal.Record, error) {
	var (
		m MessageType
		s internal.Stream
		r internal.Record
	)

	if err := json.Unmarshal([]byte(input), &m); err != nil {
		return nil, nil, err
	}

	switch m.Type {
	case "SCHEMA":
		if err := json.Unmarshal([]byte(input), &s); err != nil {
			return nil, nil, err
		}
		return &s, nil, nil
	case "RECORD":
		if err := json.Unmarshal([]byte(input), &r); err != nil {
			return nil, &r, err
		}
		return nil, &r, nil
	case "STATE":
		// save state
		return nil, nil, saveState(logger, input, stateDirectory)
	}

	return nil, nil, fmt.Errorf("unknown message type : %v ", m.Type)
}

func saveState(logger internal.Logger, input string, path string) error {
	now := time.Now()

	statePath := filepath.Join(path, fmt.Sprintf("state-%v.json", now.UnixMilli()))
	logger.Info(fmt.Sprintf("saving state to path : %v", statePath))

	if _, err := os.OpenFile(statePath, os.O_CREATE, 0644); err != nil {
		return errors.Wrap(err, "unable to create file to save state")
	}

	if err := ioutil.WriteFile(statePath, []byte(input), fs.ModePerm); err != nil {
		logger.Error(fmt.Sprintf("unable to save state to path %v", statePath))
		return errors.Wrap(err, "unable to save state")
	}
	return nil
}

type MessageType struct {
	Type string `json:"type"`
}

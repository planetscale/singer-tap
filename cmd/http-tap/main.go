package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io/fs"
	"io/ioutil"
	"os"
	"path/filepath"
	"time"

	"github.com/pkg/errors"
	"github.com/planetscale/singer-tap/cmd/internal"
)

var (
	singerAPIURL   string
	batchSize      int
	bufferSize     int
	apiToken       string
	stateDirectory string
)

func init() {
	flag.StringVar(&singerAPIURL, "api-url", "https://api.stitchdata.com", "API Url for Singer")
	flag.IntVar(&batchSize, "batch-size", 9000, "size of each batch sent to Singer, default is 9000")
	flag.StringVar(&apiToken, "api-token", "", "API Token to authenticate with Singer")
	flag.StringVar(&stateDirectory, "state-directory", "state", "Directory to save any received state, default is state/")
	flag.IntVar(&bufferSize, "buffer-size", 1024, "size of the buffer used to read lines from STDIN, default is 1024")
}

func main() {
	flag.Parse()

	logger := internal.NewLogger("HTTP Tap", os.Stdout, os.Stderr)
	err := execute(logger, singerAPIURL, batchSize, bufferSize, apiToken)
	if err != nil {
		logger.Error(err.Error())
		os.Exit(1)
	}
}

func execute(logger internal.Logger, apiUrl string, batchSize, bufferSize int, token string) error {
	if len(token) == 0 {
		return errors.New("Please specify a valid apiToken with the --api-token flag")
	}

	maxBufferSize := bufferSize * 1024
	buf := make([]byte, maxBufferSize)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(buf, maxBufferSize)

	var stream *internal.Stream

	recordCount := 0
	batchWriter := internal.NewBatchWriter(batchSize, logger, apiUrl, apiToken)
	for scanner.Scan() {
		s, r, err := parseInput(scanner.Text(), logger)
		if err != nil {
			return errors.Wrap(err, "unable to process output from STDIN")
		}

		if s != nil {
			if s != stream && stream != nil {
				// The schema we're writing out has changed, flush the messages so far.
				if batchWriter.Flush(stream); err != nil {
					return err
				}
				if recordCount > 0 {
					logger.Info(fmt.Sprintf("Published [%v] records for stream %q", recordCount, stream.Name))
				}
			}

			// we retain the catalog so we can build a BatchMessage
			stream = s
			recordCount = 0
		}

		if r != nil {
			recordCount += 1
			if err := batchWriter.Send(r, stream); err != nil {
				return err
			}
		}
	}

	if scanner.Err() != nil {
		return scanner.Err()
	}

	if recordCount > 0 {
		logger.Info(fmt.Sprintf("Published [%v] records for stream %q", recordCount, stream.Name))
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

	var st internal.WrappedState

	if err := json.Unmarshal([]byte(input), &st); err != nil {
		return err
	}

	stateFileContents, err := json.Marshal(st.Value)
	if err != nil {
		return err
	}

	statePath := filepath.Join(path, fmt.Sprintf("state-%v.json", now.UnixMilli()))
	logger.Info(fmt.Sprintf("saving state to path : %v", statePath))

	if err := ioutil.WriteFile(statePath, stateFileContents, fs.ModePerm); err != nil {
		logger.Error(fmt.Sprintf("unable to save state to path %v", statePath))
		return errors.Wrap(err, "unable to save state")
	}
	return nil
}

type MessageType struct {
	Type string `json:"type"`
}

package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/pkg/errors"
	"github.com/planetscale/singer-tap/cmd/internal"
	"os"
)

var (
	singerAPIURL string
	batchSize    int
	apiToken     string
)

func init() {
	flag.StringVar(&singerAPIURL, "api-url", "https://api.stitchdata.com", "API Url for Singer")
	flag.IntVar(&batchSize, "batch-size", 20, "size of each batch sent to Singer")
	flag.StringVar(&apiToken, "api-token", "", "API Token to authenticate with Singer")
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
		s, r, err := parseInput(scanner.Text())
		if err != nil {
			return errors.Wrap(err, "unable to identify type in STDIN")
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

	if stream != nil {
		return batchWriter.Flush(stream)
	}
	return nil
}

func parseInput(input string) (*internal.Stream, *internal.Record, error) {
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
		// do nothing
		return nil, nil, nil
	}

	return nil, nil, fmt.Errorf("unknown message type : %v ", m.Type)
}

type MessageType struct {
	Type string `json:"type"`
}
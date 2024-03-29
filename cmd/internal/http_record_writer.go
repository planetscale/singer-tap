package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/go-retryablehttp"
	"github.com/pkg/errors"
)

const (
	MaxObjectsInBatch   int = 1000
	MaxBatchRequestSize int = 2 * 1024 * 1024
)

func NewHttpRecordWriter(batchSize int, apiURL, apiToken, stateFileDir string, logger StatusLogger) RecordWriter {
	client := retryablehttp.NewClient()
	// Wait 3 seconds before retrying
	client.RetryWaitMin = 3 * time.Second
	client.Logger = nil
	return &httpBatchWriter{
		batchSize:    batchSize,
		apiURL:       apiURL,
		apiToken:     apiToken,
		client:       client,
		stateFileDir: stateFileDir,
		logger:       logger,
		messages:     make([]ImportMessage, 0, batchSize),
	}
}

type httpBatchWriter struct {
	batchSize    int
	apiURL       string
	apiToken     string
	client       *retryablehttp.Client
	messages     []ImportMessage
	stateFileDir string
	logger       StatusLogger
}

type BatchResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (h *httpBatchWriter) State(state State) error {
	now := time.Now()

	stateFileContents, err := json.Marshal(state)
	if err != nil {
		return err
	}

	statePath := filepath.Join(h.stateFileDir, fmt.Sprintf("state-%v.json", now.UnixMilli()))
	h.logger.Info(fmt.Sprintf("saving state to path : %v", statePath))

	if err := os.WriteFile(statePath, stateFileContents, fs.ModePerm); err != nil {
		h.logger.Error(fmt.Sprintf("unable to save state to path %v", statePath))
		return errors.Wrap(err, "unable to save state")
	}
	return nil
}

func (h *httpBatchWriter) Flush(stream Stream) error {
	if len(h.messages) == 0 {
		return nil
	}

	batches := getBatchMessages(h.messages, stream, MaxObjectsInBatch, MaxBatchRequestSize)
	h.logger.Info(fmt.Sprintf("flushing [%v] messages for stream %q in [%v] batches", len(h.messages), stream.Name, len(batches)))
	if len(batches) > 0 {
		h.printLastPKSynced(batches[len(batches)-1], stream)
	}

	for _, batch := range batches {
		b, err := json.Marshal(batch)
		if err != nil {
			return err
		}

		stitch, err := retryablehttp.NewRequest("POST", h.apiURL+"/v2/import/batch", bytes.NewBuffer(b))
		if err != nil {
			return err
		}
		stitch.Header.Set("Content-Type", "application/json")
		stitch.Header.Set("Authorization", "Bearer "+h.apiToken)

		stitchResponse, err := h.client.Do(stitch)
		if err != nil {
			return err
		}

		defer stitchResponse.Body.Close()

		if stitchResponse.StatusCode > 203 {
			body, err := io.ReadAll(stitchResponse.Body)
			if err != nil {
				return err
			}
			return fmt.Errorf("server request failed with %s", body)
		}

		var resp BatchResponse
		decoder := json.NewDecoder(stitchResponse.Body)
		if err := decoder.Decode(&resp); err != nil {
			return err
		}
	}
	h.messages = h.messages[:0]

	return nil
}

func (h *httpBatchWriter) printLastPKSynced(batch ImportBatch, stream Stream) {
	if len(batch.Messages) == 0 {
		return
	}

	lastRowSynced := batch.Messages[len(batch.Messages)-1]
	msg := fmt.Sprintf("Last row for table [%v] in the last batch  has primary keys : [ ", stream.TableName)
	for _, keyProp := range stream.KeyProperties {
		msg += fmt.Sprintf("{ %s : %v } ", keyProp, lastRowSynced.Data[keyProp])
	}
	msg += "]"
	h.logger.Info(msg)
}

func (h *httpBatchWriter) Record(record Record, stream Stream) error {
	h.messages = append(h.messages, createImportMessage(record))
	if len(h.messages) >= h.batchSize {
		return h.Flush(stream)
	}

	return nil
}

// getBatchMessages accepts a list of import messages
// and returns a slice of ImportBatch that can be safely uploaded.
// The rules are:
// 1. There cannot be more than 20,000 records in the request.
// 2. The size of the serialized JSON cannot be more than 20 MB.
func getBatchMessages(messages []ImportMessage, stream Stream, maxObjectsInBatch int, maxBatchSerializedSize int) []ImportBatch {
	var batches []ImportBatch
	allocated := 0
	unallocated := len(messages)

	for unallocated > 0 {
		batch := ImportBatch{
			Table:       stream.Name,
			Schema:      stream.Schema,
			Messages:    messages[allocated:],
			PrimaryKeys: stream.KeyProperties,
		}

		// reduce the size of the batch until it is an acceptable size.
		for batch.SizeOf() > maxBatchSerializedSize || len(batch.Messages) > maxObjectsInBatch {
			// keep halving the number of messages until the batch is an acceptable size.
			batch.Messages = batch.Messages[0:(len(batch.Messages) / 2)]
		}

		allocated += len(batch.Messages)
		unallocated -= len(batch.Messages)
		batches = append(batches, batch)
	}

	return batches
}

func (imb *ImportBatch) SizeOf() int {
	b, err := json.Marshal(imb)
	if err != nil {
		return 0
	}
	return len(b)
}

func createImportMessage(record Record) ImportMessage {
	now := time.Now()
	return ImportMessage{
		Action:    "upsert",
		EmittedAt: now.UnixMilli(),
		Data:      record.Data,
	}
}

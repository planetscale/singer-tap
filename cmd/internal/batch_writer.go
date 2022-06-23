package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"
)

// MaxImportBatchSize represents the maximum items we can send in each batch request to Stitch.
// Each data object in the request body cannot exceed 10,000 individual data points.
const MaxImportBatchSize = 10000

type BatchWriter interface {
	Flush(stream *Stream) error
	Send(record *Record, stream *Stream) error
}

func NewBatchWriter(batchSize int, logger Logger, apiURL, apiToken string) BatchWriter {
	return &httpBatchWriter{
		batchSize: batchSize,
		apiURL:    apiURL,
		apiToken:  apiToken,
		logger:    logger,
		client: &http.Client{
			Timeout: time.Second * 10,
		},
		messages: make([]ImportMessage, 0, batchSize),
	}
}

type httpBatchWriter struct {
	batchSize int
	apiURL    string
	apiToken  string
	logger    Logger
	client    *http.Client
	messages  []ImportMessage
}

type BatchResponse struct {
	Status  string `json:"status"`
	Message string `json:"message"`
}

func (h *httpBatchWriter) Flush(stream *Stream) error {
	if len(h.messages) == 0 {
		return nil
	}

	h.logger.Info(fmt.Sprintf("flushing [%v] messages for stream %q", len(h.messages), stream.Name))

	batch := ImportBatch{
		Table:       stream.Name,
		Schema:      stream.Schema,
		Messages:    h.messages,
		PrimaryKeys: stream.KeyProperties,
	}

	b, err := json.Marshal(batch)
	if err != nil {
		return err
	}

	stitch, err := http.NewRequest("POST", h.apiURL+"/v2/import/batch", bytes.NewBuffer(b))
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
		body, err := ioutil.ReadAll(stitchResponse.Body)
		if err != nil {
			return err
		}
		return fmt.Errorf("Server request failed with %s", body)
	}

	var resp BatchResponse
	decoder := json.NewDecoder(stitchResponse.Body)
	if err := decoder.Decode(&resp); err != nil {
		return err
	}

	h.logger.Info(fmt.Sprintf("Server response status : %q, message : %q", resp.Status, resp.Message))

	h.messages = h.messages[:0]

	return nil
}

func (h *httpBatchWriter) Send(record *Record, stream *Stream) error {
	h.messages = append(h.messages, createImportMessage(record))
	if len(h.messages) == MaxBatchSize {
		h.Flush(stream)
	}

	return nil
}

func createImportMessage(record *Record) ImportMessage {
	now := time.Now()
	return ImportMessage{
		Action:    "upsert",
		EmittedAt: now.UnixMilli(),
		Data:      record.Data,
	}
}

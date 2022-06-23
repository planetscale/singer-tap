package internal

import (
	"encoding/json"
	"fmt"
	"io"
	"time"
)

type Logger interface {
	Log(message string)
	Info(message string)
	Error(message string)
	Schema(Catalog) error
	Record(Record, string) error
}

const MaxBatchSize = 10000

func NewLogger(component string, stdout io.Writer, stderr io.Writer) Logger {
	sl := singerLogger{
		writer:        stdout,
		stderr:        stderr,
		component:     component,
		recordEncoder: json.NewEncoder(stdout),
		records:       make([]Record, 0, MaxBatchSize),
	}
	return &sl
}

type singerLogger struct {
	recordEncoder *json.Encoder
	writer        io.Writer
	stderr        io.Writer
	records       []Record
	component     string
}

func (sl *singerLogger) Info(msg string) {
	sl.Log("INFO : " + msg)
}

func (sl *singerLogger) Error(msg string) {
	sl.Log("ERROR : " + msg)
}

func (sl *singerLogger) Log(msg string) {
	fmt.Fprintln(sl.stderr, sl.component+" : "+msg)
}

func (sl *singerLogger) Schema(schema Catalog) error {
	schema.Type = "SCHEMA"
	return sl.recordEncoder.Encode(schema)
}

func (sl *singerLogger) Record(r Record, tableName string) error {
	now := time.Now()
	r.TimeExtracted = now.Format(time.RFC3339Nano)
	r.Stream = tableName
	sl.records = append(sl.records, r)
	if len(sl.records) == MaxBatchSize {
		sl.Flush()
	}
	return nil
}

func (sl *singerLogger) Flush() {
	for _, record := range sl.records {
		sl.recordEncoder.Encode(record)
	}
	sl.records = sl.records[:0]
}

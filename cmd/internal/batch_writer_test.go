package internal

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCanSplitIntoBatches(t *testing.T) {
	var messages []ImportMessage

	n := 1
	for n <= 20 {
		messages = append(messages, ImportMessage{
			Action: "upsert",
		})

		n++
	}

	stream := &Stream{}

	batches := getBatchMessages(messages, stream, 1, 100*1024*1024)
	assert.Equal(t, len(messages), len(batches))
	totalMessages := 0
	for _, batch := range batches {
		assert.Equal(t, 1, len(batch.Messages))
		totalMessages += len(batch.Messages)
	}

	assert.Equal(t, len(messages), totalMessages)
}

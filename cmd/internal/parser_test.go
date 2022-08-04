package internal

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestCanUnMarshalWrappedState(t *testing.T) {
	wrappedStateContents := []byte(`{
    "type": "STATE",
    "value":
    {
        "bookmarks":
        {
            "employees":
            {
                "shards":
                {
                    "-":
                    {
                        "cursor": "CgEtEhBpbXBvcnQtb24tc2NhbGVy"
                    }
                }
            },
            "salaries":
            {
                "shards":
                {
                    "-":
                    {
                        "cursor": "CgEtEhBpbXBvcnQtb24tc2NhbGVy"
                    }
                }
            },
            "titles":
            {
                "shards":
                {
                    "-":
                    {
                        "cursor": "CgEtEhBpbXBvcnQtb24tc2NhbGVy"
                    }
                }
            }
        }
    }
}`)
	state, err := parseSavedStateContents(wrappedStateContents)
	assert.NoError(t, err)
	assert.NotNil(t, state)
	assert.Equal(t, 3, len(state.Streams))
}

func TestCanUnMarshalState(t *testing.T) {
	wrappedStateContents := []byte(`{
    "bookmarks":
    {
        "employees":
        {
            "shards":
            {
                "-":
                {
                    "cursor": "CgEtEhBpbXBvcnQtb24tc2NhbGVy"
                }
            }
        },
        "salaries":
        {
            "shards":
            {
                "-":
                {
                    "cursor": "CgEtEhBpbXBvcnQtb24tc2NhbGVy"
                }
            }
        },
        "titles":
        {
            "shards":
            {
                "-":
                {
                    "cursor": "CgEtEhBpbXBvcnQtb24tc2NhbGVy"
                }
            }
        }
    }
}`)
	state, err := parseSavedStateContents(wrappedStateContents)
	assert.NoError(t, err)
	assert.NotNil(t, state)
	assert.Equal(t, 3, len(state.Streams))
}

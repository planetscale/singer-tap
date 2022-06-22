package internal

import (
	"encoding/base64"
	"github.com/pkg/errors"
	psdbconnect "github.com/planetscale/airbyte-source/proto/psdbconnect/v1alpha1"
	"github.com/planetscale/psdb/core/codec"
)

type ShardStates struct {
	Shards map[string]*SerializedCursor `json:"shards"`
}

type SerializedCursor struct {
	Cursor string `json:"cursor"`
}

func TableCursorToSerializedCursor(cursor *psdbconnect.TableCursor) (*SerializedCursor, error) {
	d, err := codec.DefaultCodec.Marshal(cursor)
	if err != nil {
		return nil, errors.Wrap(err, "unable to marshal table cursor to save staate.")
	}

	sc := &SerializedCursor{
		Cursor: base64.StdEncoding.EncodeToString(d),
	}
	return sc, nil
}

type Catalog struct {
	Type    string   `json:"type"`
	Streams []Stream `json:"streams,omitempty"`
}

// Stream represents the JSONSchema definition for a given database object.
// example:
// {
//  "streams": [
//    {
//      "tap_stream_id": "users",
//      "stream": "users",
//      "schema": {
//        "type": ["null", "object"],
//        "additionalProperties": false,
//        "properties": {
//          "id": {
//            "type": [
//              "null",
//              "string"
//            ],
//          },
//          "name": {
//            "type": [
//              "null",
//              "string"
//            ],
//          },
//          "date_modified": {
//            "type": [
//              "null",
//              "string"
//            ],
//            "format": "date-time",
//          }
//        }
//      }
//    }
//  ]
//}
type Stream struct {
	// The name of the stream.
	Name string `json:"stream"`

	// The unique identifier for the stream.
	// This is allowed to be different from the name of the stream
	// in order to allow for sources that have duplicate stream names.
	ID string `json:"tap_stream_id"`

	// The JSON schema for the stream.
	Schema StreamSchema `json:"schema"`

	// For a database source, the name of the table.
	TableName string `json:"table-name"`

	// Each piece of metadata has the following canonical shape:
	//{
	//  "metadata" : {
	//    "selected" : true,
	//    "some-other-metadata" : "whatever"
	//  },
	//  "breadcrumb" : ["properties", "some-field-name"]
	//}
	Metadata []Metadata `json:"metadata"`

	// A list of strings indicating which properties make up the primary key for this stream.
	// Each item in the list must be the name of a top-level property defined in the schema
	KeyProperties []string `json:"key_properties"`

	// A list of strings indicating which properties the tap is using as bookmarks.
	// Each item in the list must be the name of a top-level property defined in the schema.
	CursorProperties []string `json:"bookmark_properties"`
}

type StreamSchema struct {
	Type                    []string                  `json:"type"`
	HasAdditionalProperties bool                      `json:"additionalProperties"`
	Properties              map[string]StreamProperty `json:"properties"`
}

type StreamProperty struct {
	Types        []string `json:"type"`
	CustomFormat string   `json:"format,omitempty"`
}

type Metadata struct {
	Metadata NodeMetadata `json:"metadata"`
}

func (s *Stream) GenerateMetadata(keys []string) error {
	streamMetadata := NewMetadata()
	streamMetadata.Metadata.TableKeyProperties = keys
	streamMetadata.Metadata.ValidReplicationKeys = keys
	// need this to be an empty array since Singer needs an empty JSON array here.
	streamMetadata.Metadata.BreadCrumb = []string{}
	s.Metadata = append(s.Metadata, streamMetadata)
	for key := range s.Schema.Properties {
		propertyMetadata := NewMetadata()
		propertyMetadata.Metadata.BreadCrumb = []string{
			"properties", key,
		}
		s.Metadata = append(s.Metadata, propertyMetadata)
	}
	return nil
}
func NewMetadata() Metadata {
	return Metadata{
		Metadata: NodeMetadata{
			Inclusion: "available",
			Selected:  false,
		},
	}
}

// NodeMetadata represents the metadata for a given database object
// an example is :
// "metadata": [
//        {
//          "metadata": {
//            "inclusion": "available",
//            "table-key-properties": ["id"],
//            "selected": true,
//            "valid-replication-keys": ["date_modified"],
//            "schema-name": "users",
//          },
//          "breadcrumb": []
//        },
//        {
//          "metadata": {
//            "inclusion": "automatic"
//          },
//          "breadcrumb": ["properties", "id"]
//        },
//        {
//          "metadata": {
//            "inclusion": "available",
//            "selected": true
//          },
//          "breadcrumb": ["properties", "name"]
//        },
//        {
//          "metadata": {
//            "inclusion": "automatic"
//          },
//          "breadcrumb": ["properties", "date_modified"]
//        }
//      ]
type NodeMetadata struct {
	// Either true or false. Indicates that this node in the schema has been selected by the user for replication.
	Selected bool `json:"selected,omitempty"`

	// Either FULL_TABLE, INCREMENTAL, or LOG_BASED. The replication method to use for a stream.
	ReplicationMethod string `json:"replication-method,omitempty"`

	// The name of a property in the source to use as a "bookmark".
	// For example, this will often be an "updated-at" field or an auto-incrementing primary key (requires replication-method).
	ReplicationKey string `json:"replication-key,omitempty"`

	// Either available, automatic, or unsupported.
	// 1. "available" means the field is available for selection,
	// and the tap will only emit values for that field if it is marked with "selected": true.
	// 2. "automatic" means that the tap will emit values for the field.
	// 3. "unsupported" means that the field exists in the source data but the tap is unable to provide it.
	Inclusion string `json:"inclusion,omitempty"`

	// Either true or false.
	// Indicates if a node in the schema should be replicated if
	// a user has not expressed any opinion on whether or not to replicate it.
	SelectedByDefault bool `json:"selected-by-default,omitempty"`

	// List of the fields that could be used as replication keys.
	ValidReplicationKeys []string `json:"valid-replication-keys,omitempty"`

	// Used to force the replication method to either FULL_TABLE or INCREMENTAL.
	ForcedReplicationMethod string `json:"forced-replication-method,omitempty"`

	// List of key properties for a database table.
	TableKeyProperties []string `json:"table-key-properties,omitempty"`

	// The name of the stream.
	SchemaName string `json:"schema-name,omitempty"`

	// Either true or false. Indicates whether a stream corresponds to a database view.
	IsView bool `json:"is-view,omitempty"`

	// Name of database.
	DatabaseName string `json:"database-name,omitempty"`

	// Represents the datatype of a database column.
	SqlDataType string `json:"sql-datatype,omitempty"`

	// The breadcrumb object defines the path into the schema to the node to which the metadata belongs.
	//  Metadata for a stream will have an empty breadcrumb.
	// example for a stream: "breadcrumb": []
	// example for a property: "breadcrumb": ["properties", "id"]
	BreadCrumb []string `json:"breadcrumb"`
}

// Record messages contain the data from the data stream.
// example:
// {
//  "type": "RECORD",
//  "stream": "users",
//  "time_extracted": "2017-11-20T16:45:33.000Z",
//  "record": {
//    "id": 0,
//    "name": "Chris"
//  }
//}
type Record struct {
	// a constant with value "Record"
	Type string `json:"type"`

	// The string name of the stream
	Stream string `json:"stream"`

	// The time this record was observed in the source.
	// This should be an RFC3339 formatted date-time, like "2017-11-20T16:45:33.000Z".
	TimeExtracted string `json:"time_extracted"`

	// A JSON map containing a streamed data point
	Data map[string]interface{} `json:"record"`
}

func NewRecord() Record {
	return Record{
		Type: "RECORD",
	}
}

// State represents any previously known state about the last sync operation
// example :
// {
//  "bookmarks": {
//    "orders": {
//      "last_record": "2017-07-07T10:20:00Z"
//    },
//    "customers": {
//      "last_record": 123
//    }
//  }
//}
type State struct {
	Bookmarks map[string]Bookmark `json:"bookmarks"`
}

type Bookmark struct {
	Cursor string `json:"last_record"`
}

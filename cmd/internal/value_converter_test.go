package internal

import (
	"fmt"
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"vitess.io/vitess/go/sqltypes"
	querypb "vitess.io/vitess/go/vt/proto/query"
)

func TestCanConvert(t *testing.T) {
	intValue := strconv.AppendInt(nil, int64(int8(12)), 10)
	boolTrueValue := strconv.AppendInt(nil, int64(int8(1)), 10)
	boolfalseValue := strconv.AppendInt(nil, int64(int8(0)), 10)
	typeTests := []struct {
		name          string
		property      StreamProperty
		value         sqltypes.Value
		expectedValue interface{}
	}{
		{
			name: "integer",
			property: StreamProperty{
				Types: []string{"null", "integer"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_INT8, intValue),
			expectedValue: int64(12),
		},
		{
			name: "null_integer",
			property: StreamProperty{
				Types: []string{"null", "integer"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, []byte("NULL")),
			expectedValue: nil,
		},
		{
			name: "float64",
			property: StreamProperty{
				Types: []string{"null", "number"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_FLOAT64, []byte("3.1415927E+00")),
			expectedValue: 3.1415927,
		},
		{
			name: "null_float64",
			property: StreamProperty{
				Types: []string{"null", "number"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, []byte("NULL")),
			expectedValue: nil,
		},
		{
			name: "boolean_true",
			property: StreamProperty{
				Types: []string{"null", "boolean"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_INT64, boolTrueValue),
			expectedValue: true,
		},
		{
			name: "boolean_false",
			property: StreamProperty{
				Types: []string{"null", "boolean"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_INT64, boolfalseValue),
			expectedValue: false,
		},
		{
			name: "null_boolean",
			property: StreamProperty{
				Types: []string{"null", "number"},
			},
			value:         sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, []byte("NULL")),
			expectedValue: nil,
		},

		{
			name: "date-time",
			property: StreamProperty{
				Types:        []string{"null", "string"},
				CustomFormat: "date-time",
			},
			value:         sqltypes.MakeTrusted(querypb.Type_DATETIME, []byte("2023-03-23 14:28:21.592111")),
			expectedValue: "2023-03-23T14:28:21Z",
		},
		{
			name: "date-time-null",
			property: StreamProperty{
				Types:        []string{"null", "string"},
				CustomFormat: "date-time",
			},
			value:         sqltypes.MakeTrusted(querypb.Type_NULL_TYPE, []byte("NULL")),
			expectedValue: nil,
		},
	}

	for _, test := range typeTests {
		t.Run(fmt.Sprintf("test_convert_%v", test.name), func(it *testing.T) {
			val, err := Convert(test.property, test.value)
			require.NoError(it, err)
			assert.Equal(t, test.expectedValue, val)
		})
	}
}

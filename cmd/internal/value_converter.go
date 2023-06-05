package internal

import (
	"time"

	"vitess.io/vitess/go/sqltypes"
)

// Convert will turn the mysql representation of a value
// into its equivalent JSONSchema compatible representation.
func Convert(s StreamProperty, value sqltypes.Value) (interface{}, error) {
	// because JSON schema thinks that both int64 and floats are numbers,
	// we defer to the persisted value to switch how we serialize this value.
	if value.IsFloat() {
		f, err := value.ToFloat64()
		if err != nil {
			return nil, err
		}
		return f, nil
	}

	// after this, depend on the JSONSchema type to serialize the value.
	if s.IsDateTime() {
		return getISOTimeStamp(value), nil
	} else if s.IsInteger() || s.IsNumber() {
		if value.IsNull() {
			return nil, nil
		}
		i, err := value.ToInt64()
		if err != nil {
			return nil, err
		}
		return i, nil
	} else if s.IsBoolean() {
		if value.IsNull() {
			return nil, nil
		}
		b, err := value.ToBool()
		if err != nil {
			return nil, err
		}
		return b, nil
	}

	return value.ToString(), nil
}

func getISOTimeStamp(value sqltypes.Value) interface{} {
	if value.IsNull() {
		return nil
	}

	p, err := time.Parse("2006-01-02 15:04:05", value.ToString())
	if err != nil {
		return ""
	}
	return p.Format(time.RFC3339)
}

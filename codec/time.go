package codec

import (
	"database/sql"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

type timeStringCodec struct {
	name   string
	layout string
}

type unixTimeCodec struct {
	name string
	unit unixUnit
}

type unixUnit int

const (
	unixSeconds unixUnit = iota
	unixMillis
	unixNanos
)

func newTimeStringCodec(name, layout string) Codec {
	return timeStringCodec{name: name, layout: layout}
}

func newUnixTimeCodec(name string, unit unixUnit) Codec {
	return unixTimeCodec{name: name, unit: unit}
}

func (c timeStringCodec) Name() string {
	return c.name
}

func (c timeStringCodec) Decode(src any, target reflect.Value) error {
	if src == nil {
		resetFieldValue(target)
		return nil
	}

	text, err := normalizeStringSource(src)
	if err != nil {
		return fmt.Errorf("dbx/codec: codec %q: %w", c.name, err)
	}
	if strings.TrimSpace(text) == "" {
		resetFieldValue(target)
		return nil
	}

	parsed, err := time.Parse(c.layout, text)
	if err != nil {
		return fmt.Errorf("dbx/codec: codec %q: %w", c.name, err)
	}
	return assignDecodedValue(target, reflect.ValueOf(parsed))
}

func (c timeStringCodec) Encode(source reflect.Value) (any, error) {
	if !source.IsValid() || isNilValue(source) {
		var encoded any
		return encoded, nil
	}
	value, ok := codecValueAs[time.Time](source)
	if !ok {
		return nil, fmt.Errorf("dbx/codec: codec %q cannot encode %s as time.Time", c.name, source.Type())
	}
	return value.Format(c.layout), nil
}

func (c unixTimeCodec) Name() string {
	return c.name
}

func (c unixTimeCodec) Decode(src any, target reflect.Value) error {
	if src == nil {
		resetFieldValue(target)
		return nil
	}

	value, err := normalizeInt64Source(src)
	if err != nil {
		return fmt.Errorf("dbx/codec: codec %q: %w", c.name, err)
	}
	return assignDecodedValue(target, reflect.ValueOf(c.timeFromValue(value)))
}

func (c unixTimeCodec) Encode(source reflect.Value) (any, error) {
	if !source.IsValid() || isNilValue(source) {
		var encoded any
		return encoded, nil
	}
	value, ok := codecValueAs[time.Time](source)
	if !ok {
		return nil, fmt.Errorf("dbx/codec: codec %q cannot encode %s as time.Time", c.name, source.Type())
	}
	return c.valueFromTime(value), nil
}

func (c unixTimeCodec) timeFromValue(value int64) time.Time {
	switch c.unit {
	case unixSeconds:
		return time.Unix(value, 0)
	case unixMillis:
		return time.UnixMilli(value)
	case unixNanos:
		return time.Unix(0, value)
	default:
		return time.Unix(value, 0)
	}
}

func (c unixTimeCodec) valueFromTime(value time.Time) int64 {
	switch c.unit {
	case unixSeconds:
		return value.Unix()
	case unixMillis:
		return value.UnixMilli()
	case unixNanos:
		return value.UnixNano()
	default:
		return value.Unix()
	}
}

func normalizeInt64Source(src any) (int64, error) {
	switch v := src.(type) {
	case int64:
		return v, nil
	case int, int32, int16, int8:
		return reflect.ValueOf(v).Int(), nil
	case uint64:
		return convertUint64ToInt64(v)
	case uint32, uint16, uint8:
		return convertUnsignedToInt64(v)
	case []byte, sql.RawBytes:
		return parseInt64(fmt.Sprintf("%s", v))
	case string:
		return parseInt64(v)
	default:
		return 0, fmt.Errorf("unsupported unix time codec source %T", src)
	}
}

func parseInt64(input string) (int64, error) {
	value, err := strconv.ParseInt(strings.TrimSpace(input), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("dbx/codec: parse int64: %w", err)
	}
	return value, nil
}

func convertUint64ToInt64(u uint64) (int64, error) {
	if u > math.MaxInt64 {
		return 0, errors.New("dbx/codec: uint64 value overflows int64")
	}
	return int64(u), nil
}

func convertUnsignedToInt64(x any) (int64, error) {
	switch u := x.(type) {
	case uint32:
		return int64(u), nil
	case uint16:
		return int64(u), nil
	case uint8:
		return int64(u), nil
	default:
		return 0, fmt.Errorf("unsupported unsigned type %T", x)
	}
}

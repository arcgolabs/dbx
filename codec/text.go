package codec

import (
	"database/sql"
	"encoding"
	"errors"
	"fmt"
	"reflect"
	"slices"
)

type textCodec struct{}

func (textCodec) Name() string {
	return "text"
}

func (textCodec) Decode(src any, target reflect.Value) error {
	if src == nil {
		resetFieldValue(target)
		return nil
	}

	text, err := normalizeStringSource(src)
	if err != nil {
		return fmt.Errorf("dbx/codec: codec %q: %w", "text", err)
	}
	switch {
	case target.Kind() == reflect.String:
		target.SetString(text)
		return nil
	case isByteSliceType(target.Type()):
		target.SetBytes([]byte(text))
		return nil
	}

	unmarshaler, err := resolveTextUnmarshaler(target)
	if err != nil {
		return err
	}
	if unmarshaler == nil {
		return fmt.Errorf("dbx/codec: codec %q cannot decode into %s", "text", target.Type())
	}
	if err := unmarshaler.UnmarshalText([]byte(text)); err != nil {
		return fmt.Errorf("dbx/codec: codec %q: %w", "text", err)
	}
	return nil
}

func (textCodec) Encode(source reflect.Value) (any, error) {
	if !source.IsValid() || isNilValue(source) {
		var encoded any
		return encoded, nil
	}

	switch {
	case source.Kind() == reflect.String:
		return source.String(), nil
	case isByteSliceType(source.Type()):
		return slices.Clone(source.Bytes()), nil
	}

	if marshaler := resolveTextMarshaler(source); marshaler != nil {
		text, err := marshaler.MarshalText()
		if err != nil {
			return nil, fmt.Errorf("dbx/codec: codec %q: %w", "text", err)
		}
		return string(text), nil
	}
	if stringer := resolveStringer(source); stringer != nil {
		return stringer.String(), nil
	}
	return nil, fmt.Errorf("dbx/codec: codec %q cannot encode %s", "text", source.Type())
}

func normalizeStringSource(src any) (string, error) {
	switch value := src.(type) {
	case string:
		return value, nil
	case []byte:
		return string(value), nil
	case sql.RawBytes:
		return string(value), nil
	default:
		return "", fmt.Errorf("unsupported string codec source %T", src)
	}
}

func isByteSliceType(typ reflect.Type) bool {
	return typ.Kind() == reflect.Slice && typ.Elem().Kind() == reflect.Uint8
}

func resolveTextUnmarshaler(target reflect.Value) (encoding.TextUnmarshaler, error) {
	if !target.CanSet() {
		return nil, errors.New("dbx/codec: codec target is not settable")
	}
	if target.Kind() == reflect.Pointer {
		if target.IsNil() {
			target.Set(reflect.New(target.Type().Elem()))
		}
		if unmarshaler, ok := target.Interface().(encoding.TextUnmarshaler); ok {
			return unmarshaler, nil
		}
	}
	if target.CanAddr() {
		if unmarshaler, ok := target.Addr().Interface().(encoding.TextUnmarshaler); ok {
			return unmarshaler, nil
		}
	}
	var unmarshaler encoding.TextUnmarshaler
	return unmarshaler, nil
}

func resolveTextMarshaler(source reflect.Value) encoding.TextMarshaler {
	if !source.IsValid() {
		return nil
	}
	if marshaler, ok := source.Interface().(encoding.TextMarshaler); ok {
		return marshaler
	}
	if source.Kind() != reflect.Pointer && source.CanAddr() {
		if marshaler, ok := source.Addr().Interface().(encoding.TextMarshaler); ok {
			return marshaler
		}
	}
	return nil
}

func resolveStringer(source reflect.Value) fmt.Stringer {
	if !source.IsValid() {
		return nil
	}
	if stringer, ok := source.Interface().(fmt.Stringer); ok {
		return stringer
	}
	if source.Kind() != reflect.Pointer && source.CanAddr() {
		if stringer, ok := source.Addr().Interface().(fmt.Stringer); ok {
			return stringer
		}
	}
	return nil
}

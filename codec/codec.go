package codec

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
)

type Codec interface {
	Name() string
	Decode(src any, target reflect.Value) error
	Encode(source reflect.Value) (any, error)
}

type typedCodec[T any] struct {
	name   string
	decode func(any) (T, error)
	encode func(T) (any, error)
}

type jsonCodec struct{}

func New[T any](name string, decode func(any) (T, error), encode func(T) (any, error)) Codec {
	return typedCodec[T]{
		name:   NormalizeName(name),
		decode: decode,
		encode: encode,
	}
}

func NormalizeName(name string) string {
	return strings.ToLower(strings.TrimSpace(name))
}

func IsNil(codec Codec) bool {
	if codec == nil {
		return true
	}
	value := reflect.ValueOf(codec)
	return value.Kind() == reflect.Pointer && value.IsNil()
}

func (c typedCodec[T]) Name() string {
	return c.name
}

func (c typedCodec[T]) Decode(src any, target reflect.Value) error {
	if src == nil {
		resetFieldValue(target)
		return nil
	}

	value, err := c.decode(src)
	if err != nil {
		return err
	}
	return assignDecodedValue(target, reflect.ValueOf(value))
}

func (c typedCodec[T]) Encode(source reflect.Value) (any, error) {
	if !source.IsValid() || isNilValue(source) {
		var encoded any
		return encoded, nil
	}

	value, ok := codecValueAs[T](source)
	if !ok {
		return nil, fmt.Errorf("dbx/codec: codec %q cannot encode %s as %s", c.name, source.Type(), reflect.TypeFor[T]())
	}
	return c.encode(value)
}

func (jsonCodec) Name() string {
	return "json"
}

func (jsonCodec) Decode(src any, target reflect.Value) error {
	if src == nil {
		resetFieldValue(target)
		return nil
	}

	payload, err := normalizeJSONPayload(src)
	if err != nil {
		return err
	}
	if len(payload) == 0 {
		resetFieldValue(target)
		return nil
	}

	destination, err := codecDecodeTarget(target)
	if err != nil {
		return err
	}
	if err := json.Unmarshal(payload, destination.Interface()); err != nil {
		return fmt.Errorf("dbx/codec: codec %q: %w", "json", err)
	}
	return nil
}

func (jsonCodec) Encode(source reflect.Value) (any, error) {
	if !source.IsValid() || isNilValue(source) {
		var encoded any
		return encoded, nil
	}
	payload, err := json.Marshal(source.Interface())
	if err != nil {
		return nil, fmt.Errorf("dbx/codec: codec %q: %w", "json", err)
	}
	return payload, nil
}

func codecValueAs[T any](source reflect.Value) (T, bool) {
	var zero T
	if !source.IsValid() {
		return zero, false
	}
	if value, ok := source.Interface().(T); ok {
		return value, true
	}
	if source.Kind() == reflect.Pointer && !source.IsNil() {
		if value, ok := source.Elem().Interface().(T); ok {
			return value, true
		}
	}
	return zero, false
}

func assignDecodedValue(target, value reflect.Value) error {
	if !target.CanSet() {
		return errors.New("dbx/codec: codec target is not settable")
	}
	if !value.IsValid() {
		resetFieldValue(target)
		return nil
	}

	if target.Kind() == reflect.Pointer {
		return assignToPointerTarget(target, value)
	}
	return assignToNonPointerTarget(target, value)
}

func assignToPointerTarget(target, value reflect.Value) error {
	if value.Type().AssignableTo(target.Type()) {
		target.Set(value)
		return nil
	}
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			resetFieldValue(target)
			return nil
		}
		value = value.Elem()
	}
	holder := reflect.New(target.Type().Elem())
	if err := assignDecodedValue(holder.Elem(), value); err != nil {
		return err
	}
	target.Set(holder)
	return nil
}

func assignToNonPointerTarget(target, value reflect.Value) error {
	if value.Kind() == reflect.Pointer {
		if value.IsNil() {
			resetFieldValue(target)
			return nil
		}
		value = value.Elem()
	}
	if value.Type().AssignableTo(target.Type()) {
		target.Set(value)
		return nil
	}
	if value.Type().ConvertibleTo(target.Type()) {
		target.Set(value.Convert(target.Type()))
		return nil
	}
	return fmt.Errorf("dbx/codec: decoded codec value %s cannot be assigned to %s", value.Type(), target.Type())
}

func codecDecodeTarget(target reflect.Value) (reflect.Value, error) {
	if !target.CanSet() {
		return reflect.Value{}, errors.New("dbx/codec: codec target is not settable")
	}
	if target.Kind() == reflect.Pointer {
		if target.IsNil() {
			target.Set(reflect.New(target.Type().Elem()))
		}
		return target, nil
	}
	return target.Addr(), nil
}

func resetFieldValue(target reflect.Value) {
	if target.CanSet() {
		target.Set(reflect.Zero(target.Type()))
	}
}

func isNilValue(value reflect.Value) bool {
	kind := value.Kind()
	if kind == reflect.Pointer || kind == reflect.Map || kind == reflect.Slice || kind == reflect.Interface {
		return value.IsNil()
	}
	return false
}

func normalizeJSONPayload(src any) ([]byte, error) {
	switch value := src.(type) {
	case []byte:
		return slices.Clone(value), nil
	case sql.RawBytes:
		return slices.Clone(value), nil
	case string:
		return []byte(value), nil
	default:
		return nil, fmt.Errorf("dbx/codec: json codec does not support source type %T", src)
	}
}

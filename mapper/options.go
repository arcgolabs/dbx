package mapper

import (
	"fmt"

	"github.com/arcgolabs/collectionx"
	codecx "github.com/arcgolabs/dbx/codec"
	"github.com/arcgolabs/pkg/option"
)

type MapperOption func(*mapperBuildOptions) error

type mapperBuildOptions struct {
	runtime *mapperRuntime
}

func WithMapperCodecs(codecs ...codecx.Codec) MapperOption {
	return func(opts *mapperBuildOptions) error {
		filtered := collectionx.FilterList[codecx.Codec](collectionx.NewList[codecx.Codec](codecs...), func(_ int, codec codecx.Codec) bool {
			return !codecx.IsNil(codec)
		})
		if filtered.Len() == 0 {
			return nil
		}

		runtime := opts.runtime.clone()
		var registerErr error
		filtered.Range(func(_ int, codec codecx.Codec) bool {
			if err := runtime.codecs.Register(codec); err != nil {
				registerErr = fmt.Errorf("register mapper codec: %w", err)
				return false
			}
			return true
		})
		if registerErr != nil {
			return registerErr
		}
		opts.runtime = runtime
		return nil
	}
}

func defaultMapperBuildOptions() mapperBuildOptions {
	return mapperBuildOptions{
		runtime: defaultMapperRuntime,
	}
}

func applyMapperOptions(opts ...MapperOption) (mapperBuildOptions, error) {
	config := defaultMapperBuildOptions()
	if err := option.ApplyErr(&config, opts...); err != nil {
		return mapperBuildOptions{}, fmt.Errorf("dbx: apply mapper options: %w", err)
	}
	return config, nil
}

func (r *mapperRuntime) clone() *mapperRuntime {
	if r == nil {
		return newMapperRuntime()
	}
	cloned := &mapperRuntime{
		registry: newMapperRegistry(),
		codecs:   r.codecs.Clone(),
	}
	return cloned
}

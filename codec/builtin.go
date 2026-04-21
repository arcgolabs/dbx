package codec

import "time"

func registerBuiltins(registry *Registry) {
	if registry == nil {
		return
	}
	registry.MustRegister(jsonCodec{})
	registry.MustRegister(textCodec{})
	registry.MustRegister(newTimeStringCodec("rfc3339_time", time.RFC3339))
	registry.MustRegister(newTimeStringCodec("rfc3339nano_time", time.RFC3339Nano))
	registry.MustRegister(newUnixTimeCodec("unix_time", unixSeconds))
	registry.MustRegister(newUnixTimeCodec("unix_milli_time", unixMillis))
	registry.MustRegister(newUnixTimeCodec("unix_nano_time", unixNanos))
}

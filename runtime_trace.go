package dbx

import "log/slog"

type runtimeTraceSession interface {
	Logger() *slog.Logger
	Debug() bool
}

func logRuntimeNode(session Session, node string, attrs ...any) {
	if session == nil {
		return
	}
	traced, ok := session.(runtimeTraceSession)
	if !ok || traced == nil || !traced.Debug() || traced.Logger() == nil {
		return
	}
	logRuntimeNodeWithLogger(traced.Logger(), traced.Debug(), node, attrs...)
}

// LogRuntimeNode emits a debug-level runtime node log for the provided session when debug logging is enabled.
// Intended for dbx subpackages such as repository.
func LogRuntimeNode(session Session, node string, attrs ...any) {
	logRuntimeNode(session, node, attrs...)
}

func logRuntimeNodeWithLogger(logger *slog.Logger, debug bool, node string, attrs ...any) {
	if logger == nil || !debug {
		return
	}
	fields := make([]any, 0, len(attrs)+2)
	fields = append(fields, "node", node)
	fields = append(fields, attrs...)
	logger.Debug("dbx runtime node", fields...)
}

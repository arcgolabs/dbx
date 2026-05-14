package audit

import (
	"context"
	"sync"
	"time"
)

type revisionInfoKey struct{}
type revisionScopeKey struct{}

// RevisionInfo contains optional metadata copied into revision rows.
type RevisionInfo struct {
	Actor     string
	Tenant    string
	Reason    string
	Metadata  any
	CreatedAt time.Time
}

type revisionScope struct {
	mu      sync.Mutex
	records map[string]RevisionRecord
}

// WithActor stores the revision actor in ctx.
func WithActor(ctx context.Context, actor string) context.Context {
	return withRevisionInfo(ctx, func(info *RevisionInfo) { info.Actor = actor })
}

// WithTenant stores the revision tenant in ctx.
func WithTenant(ctx context.Context, tenant string) context.Context {
	return withRevisionInfo(ctx, func(info *RevisionInfo) { info.Tenant = tenant })
}

// WithReason stores the revision reason in ctx.
func WithReason(ctx context.Context, reason string) context.Context {
	return withRevisionInfo(ctx, func(info *RevisionInfo) { info.Reason = reason })
}

// WithMetadata stores revision metadata in ctx. The value is bound directly to
// the configured metadata column.
func WithMetadata(ctx context.Context, metadata any) context.Context {
	return withRevisionInfo(ctx, func(info *RevisionInfo) { info.Metadata = metadata })
}

// WithCreatedAt overrides the revision timestamp for deterministic callers and tests.
func WithCreatedAt(ctx context.Context, createdAt time.Time) context.Context {
	return withRevisionInfo(ctx, func(info *RevisionInfo) { info.CreatedAt = createdAt })
}

// WithRevisionScope makes all audit writes in ctx reuse one revision row per revision table.
func WithRevisionScope(ctx context.Context) context.Context {
	ctx = normalizeContext(ctx)
	if _, ok := ctx.Value(revisionScopeKey{}).(*revisionScope); ok {
		return ctx
	}
	return context.WithValue(ctx, revisionScopeKey{}, &revisionScope{records: make(map[string]RevisionRecord)})
}

func revisionInfoFromContext(ctx context.Context) RevisionInfo {
	if ctx == nil {
		return RevisionInfo{}
	}
	info, ok := ctx.Value(revisionInfoKey{}).(RevisionInfo)
	if !ok {
		return RevisionInfo{}
	}
	return info
}

func withRevisionInfo(ctx context.Context, update func(*RevisionInfo)) context.Context {
	ctx = normalizeContext(ctx)
	info := revisionInfoFromContext(ctx)
	if update != nil {
		update(&info)
	}
	return context.WithValue(ctx, revisionInfoKey{}, info)
}

func revisionScopeFromContext(ctx context.Context) (*revisionScope, bool) {
	if ctx == nil {
		return nil, false
	}
	scope, ok := ctx.Value(revisionScopeKey{}).(*revisionScope)
	return scope, ok
}

func normalizeContext(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

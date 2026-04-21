package idgen_test

import (
	"context"
	"testing"

	"github.com/arcgolabs/dbx/idgen"
)

func TestDefaultGeneratorDispatchesStrategies(t *testing.T) {
	generator := mustNewDefaultGenerator(t)

	cases := []struct {
		name     string
		request  idgen.Request
		validate func(any) bool
	}{
		{
			name:     "snowflake",
			request:  idgen.Request{Strategy: idgen.StrategySnowflake},
			validate: func(value any) bool { id, ok := value.(int64); return ok && id > 0 },
		},
		{
			name:     "uuid",
			request:  idgen.Request{Strategy: idgen.StrategyUUID, UUIDVersion: idgen.DefaultUUIDVersion},
			validate: func(value any) bool { id, ok := value.(string); return ok && id != "" },
		},
		{
			name:     "ulid",
			request:  idgen.Request{Strategy: idgen.StrategyULID},
			validate: func(value any) bool { id, ok := value.(string); return ok && id != "" },
		},
		{
			name:     "ksuid",
			request:  idgen.Request{Strategy: idgen.StrategyKSUID},
			validate: func(value any) bool { id, ok := value.(string); return ok && id != "" },
		},
	}

	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			requireGeneratedID(t, generator, tt.request, tt.validate)
		})
	}
}

func TestSnowflakeGeneratorRejectsOtherStrategies(t *testing.T) {
	generator, err := idgen.NewSnowflake(idgen.DefaultNodeID)
	if err != nil {
		t.Fatalf("NewSnowflake returned error: %v", err)
	}

	if _, err := generator.GenerateID(context.Background(), idgen.Request{Strategy: idgen.StrategyUUID}); err == nil {
		t.Fatal("expected snowflake generator to reject uuid strategy")
	}
}

func mustNewDefaultGenerator(t *testing.T) idgen.Generator {
	t.Helper()
	generator, err := idgen.NewDefault(idgen.DefaultNodeID)
	if err != nil {
		t.Fatalf("NewDefault returned error: %v", err)
	}
	return generator
}

func requireGeneratedID(
	t *testing.T,
	generator idgen.Generator,
	request idgen.Request,
	validate func(any) bool,
) {
	t.Helper()
	id, err := generator.GenerateID(context.Background(), request)
	if err != nil {
		t.Fatalf("GenerateID returned error: %v", err)
	}
	if !validate(id) {
		t.Fatalf("unexpected generated id: %#v", id)
	}
}

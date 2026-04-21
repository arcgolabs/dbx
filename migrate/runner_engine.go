package migrate

import (
	"context"
	"database/sql"
	"fmt"
	"strconv"

	"github.com/DaiYuANg/arcgo/collectionx"
	"github.com/pressly/goose/v3"
	"github.com/samber/lo"
)

type runnerEngine struct {
	runner        *Runner
	engine        *goose.Provider
	metaByVersion collectionx.Map[int64, AppliedRecord]
}

type goEngineBuildState struct {
	gooseMigrations []*goose.Migration
	metaByVersion   collectionx.Map[int64, AppliedRecord]
}

type sqlEngineBuildState struct {
	gooseMigrations []*goose.Migration
	metaByVersion   collectionx.Map[int64, AppliedRecord]
	repeatables     collectionx.List[loadedSQLMigration]
}

func (r *Runner) newRunnerEngineForGo(migrations []Migration) (*runnerEngine, error) {
	if len(migrations) == 0 {
		return &runnerEngine{runner: r, metaByVersion: collectionx.NewMap[int64, AppliedRecord]()}, nil
	}

	state, err := lo.ReduceErr(migrations, func(state goEngineBuildState, migration Migration, _ int) (goEngineBuildState, error) {
		version, parseErr := parseNumericVersion(migration.Version())
		if parseErr != nil {
			return goEngineBuildState{}, fmt.Errorf("dbx/migrate: parse go migration version %q: %w", migration.Version(), parseErr)
		}

		state.gooseMigrations = lo.Concat(state.gooseMigrations, []*goose.Migration{goose.NewGoMigration(
			version,
			&goose.GoFunc{RunTx: migration.Up},
			&goose.GoFunc{RunTx: migration.Down},
		)})
		state.metaByVersion.Set(version, AppliedRecord{
			Version:     migration.Version(),
			Description: migration.Description(),
			Kind:        KindGo,
			Checksum:    checksumGoMigration(migration),
			Success:     true,
		})
		return state, nil
	}, goEngineBuildState{
		gooseMigrations: make([]*goose.Migration, 0, len(migrations)),
		metaByVersion:   collectionx.NewMapWithCapacity[int64, AppliedRecord](len(migrations)),
	})
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: build go migration engine state: %w", err)
	}

	return r.newRunnerEngine(state.gooseMigrations, state.metaByVersion)
}

func (r *Runner) newRunnerEngineForSQL(source FileSource) (*runnerEngine, collectionx.List[loadedSQLMigration], error) {
	loaded, err := loadSQLMigrations(source)
	if err != nil {
		return nil, nil, err
	}
	if loaded.Len() == 0 {
		return nil, nil, nil
	}

	state, err := collectionx.ReduceErrList[loadedSQLMigration, sqlEngineBuildState](loaded, sqlEngineBuildState{
		gooseMigrations: make([]*goose.Migration, 0, loaded.Len()),
		metaByVersion:   collectionx.NewMapWithCapacity[int64, AppliedRecord](loaded.Len()),
		repeatables:     collectionx.NewListWithCapacity[loadedSQLMigration](loaded.Len()),
	}, func(state sqlEngineBuildState, _ int, migration loadedSQLMigration) (sqlEngineBuildState, error) {
		if migration.kind == KindRepeatable {
			state.repeatables.Add(migration)
			return state, nil
		}

		version, versionErr := parseNumericVersion(migration.Version)
		if versionErr != nil {
			return sqlEngineBuildState{}, fmt.Errorf("dbx/migrate: parse sql migration version %q: %w", migration.Version, versionErr)
		}

		state.gooseMigrations = lo.Concat(state.gooseMigrations, []*goose.Migration{goose.NewGoMigration(
			version,
			runTxSQL(migration.upSQL),
			runTxSQL(migration.downSQL),
		)})
		state.metaByVersion.Set(version, AppliedRecord{
			Version:     migration.Version,
			Description: migration.Description,
			Kind:        migration.kind,
			Checksum:    migration.checksum,
			Success:     true,
		})
		return state, nil
	})
	if err != nil {
		return nil, nil, fmt.Errorf("dbx/migrate: build sql migration engine state: %w", err)
	}

	if len(state.gooseMigrations) == 0 {
		return nil, state.repeatables, nil
	}
	engine, err := r.newRunnerEngine(state.gooseMigrations, state.metaByVersion)
	if err != nil {
		return nil, nil, err
	}
	return engine, state.repeatables, nil
}

func (r *Runner) newRunnerEngine(migrations []*goose.Migration, metaByVersion collectionx.Map[int64, AppliedRecord]) (*runnerEngine, error) {
	if len(migrations) == 0 {
		return &runnerEngine{
			runner:        r,
			metaByVersion: metaByVersion,
		}, nil
	}

	engine, err := goose.NewProvider(
		goose.DialectCustom,
		r.db,
		nil,
		goose.WithStore(newHistoryStore(r.dialect, r.options.HistoryTable, metaByVersion)),
		goose.WithDisableGlobalRegistry(true),
		goose.WithAllowOutofOrder(r.options.AllowOutOfOrder),
		goose.WithGoMigrations(migrations...),
	)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: create goose provider: %w", err)
	}
	return &runnerEngine{
		runner:        r,
		engine:        engine,
		metaByVersion: metaByVersion,
	}, nil
}

func runTxSQL(statement string) *goose.GoFunc {
	if statement == "" {
		return nil
	}
	return &goose.GoFunc{
		RunTx: func(ctx context.Context, tx *sql.Tx) error {
			_, err := tx.ExecContext(ctx, statement)
			if err != nil {
				return fmt.Errorf("dbx/migrate: execute sql migration statement: %w", err)
			}
			return nil
		},
	}
}

func parseNumericVersion(version string) (int64, error) {
	parsed, err := strconv.ParseInt(version, 10, 64)
	if err != nil || parsed < 1 {
		return 0, fmt.Errorf("dbx/migrate: goose requires a positive numeric version, got %q", version)
	}
	return parsed, nil
}

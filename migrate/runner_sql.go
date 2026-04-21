package migrate

import (
	"fmt"
	"io/fs"
	"strings"

	"github.com/DaiYuANg/arcgo/collectionx"
)

type loadedSQLMigration struct {
	SQLMigration
	kind     Kind
	upSQL    string
	downSQL  string
	checksum string
}

func loadSQLMigrations(source FileSource) (collectionx.List[loadedSQLMigration], error) {
	items, err := source.List()
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: list sql migrations: %w", err)
	}

	loaded, err := collectionx.ReduceErrList[SQLMigration, collectionx.List[loadedSQLMigration]](items, collectionx.NewListWithCapacity[loadedSQLMigration](items.Len()), func(result collectionx.List[loadedSQLMigration], _ int, migration SQLMigration) (collectionx.List[loadedSQLMigration], error) {
		if migration.UpPath == "" {
			return result, nil
		}
		item, loadErr := loadSQLMigration(source.FS, migration)
		if loadErr != nil {
			return nil, loadErr
		}
		result.Add(item)
		return result, nil
	})
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: load sql migrations: %w", err)
	}
	return loaded, nil
}

func loadSQLMigration(fsys fs.FS, migration SQLMigration) (loadedSQLMigration, error) {
	upSQL, err := readSQLFile(fsys, migration.UpPath)
	if err != nil {
		return loadedSQLMigration{}, err
	}

	downSQL := ""
	if migration.DownPath != "" {
		downSQL, err = readSQLFile(fsys, migration.DownPath)
		if err != nil {
			return loadedSQLMigration{}, err
		}
	}

	return loadedSQLMigration{
		SQLMigration: migration,
		kind:         kindForSQLMigration(migration),
		upSQL:        upSQL,
		downSQL:      downSQL,
		checksum:     checksumSQLMigration(migration, upSQL, downSQL),
	}, nil
}

func readSQLFile(fsys fs.FS, path string) (string, error) {
	bytes, err := fs.ReadFile(fsys, path)
	if err != nil {
		return "", fmt.Errorf("dbx/migrate: read sql file %q: %w", path, err)
	}
	return strings.TrimSpace(string(bytes)), nil
}

func kindForSQLMigration(migration SQLMigration) Kind {
	if migration.Repeatable {
		return KindRepeatable
	}
	return KindSQL
}

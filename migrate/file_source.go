package migrate

import (
	"errors"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	collectionx "github.com/arcgolabs/collectionx/list"
	mappingx "github.com/arcgolabs/collectionx/mapping"
)

// ErrInvalidVersionedFilename reports an invalid versioned migration filename.
var ErrInvalidVersionedFilename = errors.New("dbx/migrate: invalid versioned filename")

const versionedMigrationSuffix = ".sql"

// ParseVersionedFilename parses a migration filename into a structured record.
func ParseVersionedFilename(name string) (VersionedFile, error) {
	base, header, prefix, err := parseVersionedFilenameHeader(name)
	if err != nil {
		return VersionedFile{}, err
	}
	version, rawDescription, ok := strings.Cut(header, "__")
	if !ok {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}

	file := VersionedFile{
		Version:  version,
		Filename: base,
		Path:     name,
	}
	if err := setVersionedFileKind(&file, prefix); err != nil {
		return VersionedFile{}, err
	}

	description, database := splitVersionedDescription(rawDescription)
	if !isValidVersion(file.Version) && file.Kind != KindRepeatable {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}
	if description == "" {
		return VersionedFile{}, ErrInvalidVersionedFilename
	}
	file.Description = strings.ReplaceAll(description, "_", " ")
	file.Database = database
	return file, nil
}

func parseVersionedFilenameHeader(name string) (string, string, byte, error) {
	base := filepath.Base(name)
	if len(base) < len("V__a.sql") {
		return "", "", 0, ErrInvalidVersionedFilename
	}
	if !strings.HasSuffix(base, versionedMigrationSuffix) {
		return "", "", 0, ErrInvalidVersionedFilename
	}
	prefix := base[0]
	header := base[1 : len(base)-len(versionedMigrationSuffix)]
	return base, header, prefix, nil
}

func setVersionedFileKind(file *VersionedFile, prefix byte) error {
	switch prefix {
	case 'V':
		return setVersionedSQLKind(file, DirectionUp)
	case 'U':
		return setVersionedSQLKind(file, DirectionDown)
	case 'R':
		if file.Version != "" {
			return ErrInvalidVersionedFilename
		}
		file.Kind = KindRepeatable
		file.Direction = DirectionUp
		return nil
	default:
		return ErrInvalidVersionedFilename
	}
}

func setVersionedSQLKind(file *VersionedFile, direction Direction) error {
	if file.Version == "" {
		return ErrInvalidVersionedFilename
	}
	file.Kind = KindSQL
	file.Direction = direction
	return nil
}

func splitVersionedDescription(raw string) (string, DialectName) {
	for _, separator := range []string{"__", "_"} {
		last := strings.LastIndex(raw, separator)
		if last <= 0 {
			continue
		}

		database := strings.TrimSpace(raw[last+len(separator):])
		if !isDialectName(database) {
			continue
		}

		return raw[:last], normalizeMigrationDatabase(database)
	}

	return raw, DialectAny
}

func isDialectName(raw string) bool {
	if raw == "" {
		return false
	}
	_, err := ParseDialectName(raw)
	return err == nil
}

func isValidVersion(version string) bool {
	if version == "" {
		return false
	}
	return !strings.ContainsFunc(version, func(ch rune) bool {
		return !isVersionRune(ch)
	})
}

func isVersionRune(ch rune) bool {
	return (ch >= '0' && ch <= '9') ||
		(ch >= 'A' && ch <= 'Z') ||
		(ch >= 'a' && ch <= 'z') ||
		ch == '_' || ch == '.' || ch == '-'
}

// List returns the SQL migrations discovered in s.
func (s FileSource) List() (*collectionx.List[SQLMigration], error) {
	entries, err := s.readEntries()
	if err != nil {
		return nil, err
	}

	items := mappingx.NewMapWithCapacity[string, *SQLMigration](entries.Len())
	_, err = collectionx.ReduceErrList[fs.DirEntry, struct{}](
		collectionx.FilterList[fs.DirEntry](entries, func(_ int, entry fs.DirEntry) bool {
			return !entry.IsDir()
		}),
		struct{}{},
		func(state struct{}, _ int, entry fs.DirEntry) (struct{}, error) {
			return state, s.addEntry(items, entry)
		},
	)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: collect sql migration entries: %w", err)
	}

	return sortedSQLMigrations(items), nil
}

func (s FileSource) readEntries() (*collectionx.List[fs.DirEntry], error) {
	entries, err := fs.ReadDir(s.FS, s.Dir)
	if err != nil {
		return nil, fmt.Errorf("dbx/migrate: read migration dir %q: %w", s.Dir, err)
	}
	return collectionx.NewList[fs.DirEntry](entries...), nil
}

func (s FileSource) addEntry(items *mappingx.Map[string, *SQLMigration], entry fs.DirEntry) error {
	fullPath, err := safeJoinPath(s.Dir, entry.Name())
	if err != nil {
		return fmt.Errorf("dbx/migrate: resolve migration path %q: %w", entry.Name(), err)
	}

	parsed, err := ParseVersionedFilename(entry.Name())
	if err != nil {
		if errors.Is(err, ErrInvalidVersionedFilename) {
			return nil
		}
		return fmt.Errorf("dbx/migrate: parse migration filename %q: %w", entry.Name(), err)
	}
	if !selectsVersionedFile(parsed, s.Database) {
		return nil
	}

	key := sqlMigrationKey(parsed)
	migration, exists := items.Get(key)
	if !exists {
		migration = &SQLMigration{
			Version:     parsed.Version,
			Description: parsed.Description,
			Database:    parsed.Database,
			Repeatable:  parsed.Kind == KindRepeatable,
		}
		items.Set(key, migration)
	}

	setSQLMigrationPath(migration, parsed.Direction, filepath.ToSlash(fullPath))
	return nil
}

func sqlMigrationKey(file VersionedFile) string {
	return strings.Join([]string{file.Version, file.Description, file.Database.String()}, ":")
}

func normalizeMigrationDatabase(name string) DialectName {
	normalized, err := ParseDialectName(name)
	if err != nil {
		return DialectAny
	}
	return normalized
}

func normalizeMigrationDatabases(databases []DialectName) []DialectName {
	if len(databases) == 0 {
		return nil
	}

	normed := make([]DialectName, 0, len(databases))
	seen := make(map[DialectName]struct{}, len(databases))
	for _, database := range databases {
		if !database.IsKnown() {
			continue
		}
		if _, ok := seen[database]; ok {
			continue
		}
		seen[database] = struct{}{}
		normed = append(normed, database)
	}
	return normed
}

func selectsVersionedFile(file VersionedFile, selectedDatabase DialectName) bool {
	if selectedDatabase.IsAny() {
		return true
	}
	return file.Database.IsAny() || file.Database == selectedDatabase
}

func setSQLMigrationPath(migration *SQLMigration, direction Direction, fullPath string) {
	if direction == DirectionUp {
		migration.UpPath = fullPath
		return
	}
	migration.DownPath = fullPath
}

func sortedSQLMigrations(items *mappingx.Map[string, *SQLMigration]) *collectionx.List[SQLMigration] {
	sorted := collectionx.NewListWithCapacity[SQLMigration](items.Len())
	items.Range(func(_ string, migration *SQLMigration) bool {
		sorted.Add(*migration)
		return true
	})
	return sorted.Sort(func(left, right SQLMigration) int {
		switch {
		case left.Repeatable != right.Repeatable:
			if left.Repeatable {
				return 1
			}
			return -1
		case left.Version < right.Version:
			return -1
		case left.Version > right.Version:
			return 1
		case left.Description < right.Description:
			return -1
		case left.Description > right.Description:
			return 1
		default:
			return 0
		}
	})
}

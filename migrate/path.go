package migrate

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
)

// safeJoinPath joins base and name, returning an error if the result escapes base.
func safeJoinPath(base, name string) (string, error) {
	base = filepath.Clean(base)
	path := filepath.Clean(filepath.Join(base, name))
	rel, err := filepath.Rel(base, path)
	if err != nil {
		return "", fmt.Errorf("dbx/migrate: compute relative path for %q: %w", name, err)
	}
	if strings.HasPrefix(rel, "..") {
		return "", errors.New("path traversal not allowed: " + name)
	}
	return path, nil
}

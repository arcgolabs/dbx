package schemamigrate

import "fmt"

func wrapMigrateError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dbx/schemamigrate: %s: %w", op, err)
}

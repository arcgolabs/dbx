package mapper

import "fmt"

func wrapDBError(op string, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("dbx/mapper: %s: %w", op, err)
}

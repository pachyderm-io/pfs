package collection

import (
	"fmt"
	"strings"

	"github.com/pachyderm/pachyderm/v2/src/internal/errors"
)

// ErrNotFound indicates that a key was not found when it was expected to
// exist.
type ErrNotFound struct {
	Type string
	Key  string
}

func (err ErrNotFound) Is(other error) bool {
	_, ok := other.(ErrNotFound)
	return ok
}

func (err ErrNotFound) Error() string {
	return fmt.Sprintf("%s %s not found", strings.TrimPrefix(err.Type, DefaultPrefix), err.Key)
}

// IsErrNotFound determines if an error is an ErrNotFound error
func IsErrNotFound(err error) bool {
	return errors.Is(err, ErrNotFound{})
}

// ErrExists indicates that a key was found to exist when it was expected not
// to.
type ErrExists struct {
	Type string
	Key  string
}

func (err ErrExists) Is(other error) bool {
	_, ok := other.(ErrExists)
	return ok
}

func (err ErrExists) Error() string {
	return fmt.Sprintf("%s %s already exists", strings.TrimPrefix(err.Type, DefaultPrefix), err.Key)
}

// IsErrExists determines if an error is an ErrExists error
func IsErrExists(err error) bool {
	return errors.Is(err, ErrExists{})
}

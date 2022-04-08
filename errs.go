package octobe

import (
	"errors"
	"fmt"
	"strings"
)

type errs []error

func (e errs) Error() string {
	var asStr []string
	for _, x := range e {
		if x == nil {
			continue
		}
		asStr = append(asStr, fmt.Sprintf("%s.", x.Error()))
	}
	return strings.Join(asStr, " ")
}

func (e errs) Is(target error) bool {
	for _, candidate := range e {
		if errors.Is(candidate, target) {
			return true
		}
	}
	return false
}

func (e errs) As(target interface{}) bool {
	for _, candidate := range e {
		if errors.As(candidate, target) {
			return true
		}
	}
	return false
}

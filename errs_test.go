package octobe

import (
	"fmt"
	"testing"

	"errors"

	"github.com/stretchr/testify/assert"
)

func TestErrs(t *testing.T) {
	e1 := errs{ErrNeedInput, ErrUsed, fmt.Errorf("test %w", errors.New("test fmt.Errorf")), nil}
	assert.ErrorIs(t, e1, ErrNeedInput)
	assert.ErrorIs(t, e1, ErrUsed)
	assert.Equal(t, "insert method require at least one argument. this query has already executed. test test fmt.Errorf.", e1.Error())
}

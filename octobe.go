package octobe

import (
	"context"
	"errors"
)

var ErrAlreadyUsed = errors.New("query already used")

// Option is a signature that can be used for passing options to a driver
type Option[CONFIG any] func(cfg *CONFIG)

// Driver is a signature that holds the specific driver in the Octobe context.
type Driver[DRIVER any, CONFIG any, BUILDER any] interface {
	Begin(ctx context.Context, opts ...Option[CONFIG]) (Session[BUILDER], error)
}

// Open is a signature that can be used for opening a driver, it should always return a driver with set signature of
// types for the local driver.
type Open[DRIVER any, CONFIG any, BUILDER any] func() (Driver[DRIVER, CONFIG, BUILDER], error)

// Octobe struct that holds the database session
type Octobe[DRIVER any, CONFIG any, BUILDER any] struct {
	driver Driver[DRIVER, CONFIG, BUILDER]
}

// New creates a new Octobe instance.
func New[DRIVER any, CONFIG any, BUILDER any](init Open[DRIVER, CONFIG, BUILDER]) (*Octobe[DRIVER, CONFIG, BUILDER], error) {
	driver, err := init()
	if err != nil {
		return nil, err
	}

	return &Octobe[DRIVER, CONFIG, BUILDER]{
		driver: driver,
	}, nil
}

// Begin a new session of queries, this will return a Session instance that can be used for handling queries. Options can be
// passed to the driver for specific configuration that overwrites the default configuration given at instantiation of
// the Octobe instance.
func (ob *Octobe[DRIVER, CONFIG, BUILDER]) Begin(ctx context.Context, opts ...Option[CONFIG]) (Session[BUILDER], error) {
	return ob.driver.Begin(ctx, opts...)
}

// Session is a signature that has a
type Session[BUILDER any] interface {
	Commit() error
	Rollback() error
	WatchRollback(func() error)
	Builder() BUILDER
}

// Handler is a signature type for a handler. The handler receives a builder of the specific driver and returns a result
// and an error.
type Handler[BUILDER any, RESULT any] func(BUILDER) (RESULT, error)

// Execute is a function that can be used for executing a handler with a session builder. This function injects the
// builder of the driver into the handler.
func Execute[BUILDER any, RESULT any](session Session[BUILDER], f Handler[BUILDER, RESULT]) (RESULT, error) {
	return f(session.Builder())
}

// Void is a type that can be used for returning nothing from a handler.
type Void *struct{}

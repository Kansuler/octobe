package postgres

import (
	"context"
	"errors"
	"github.com/Kansuler/octobe/v2"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// postgres holds the connection pool and default configuration for the postgres driver
type postgres struct {
	pool *pgx.Conn
	cfg  config
}

// config defined various configurations possible for the postgres driver
type config struct {
	txOptions *TxOptions
}

// TxOptions is a struct that holds the options for a transaction
type TxOptions pgx.TxOptions

// WithTransaction enables the use of a transaction for the session, enforce the usage of commit and rollback.
func WithTransaction(options TxOptions) octobe.Option[config] {
	return func(c *config) {
		c.txOptions = &options
	}
}

// WithoutTransaction disables the use of a transaction for the session, this will not enforce the usage of commit and
// rollback.
func WithoutTransaction() octobe.Option[config] {
	return func(c *config) {
		c.txOptions = nil
	}
}

// Type check to make sure that the postgres driver implements the Octobe Driver interface
var _ octobe.Driver[postgres, config, Builder] = &postgres{}

// Open is a function that can be used for opening a new database connection, it should always return a driver with set
// signature of types for the local driver.
func Open(ctx context.Context, dsn string, opts ...octobe.Option[config]) octobe.Open[postgres, config, Builder] {
	return func() (octobe.Driver[postgres, config, Builder], error) {
		pool, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		var cfg config
		for _, opt := range opts {
			opt(&cfg)
		}

		return &postgres{
			pool: pool,
			cfg:  cfg,
		}, nil
	}
}

// OpenWithPool is a function that can be used for opening a new database connection, it should always return a driver
// with set signature of types for the local driver. This function is used when a connection pool is already available.
func OpenWithPool(pool *pgx.Conn, opts ...octobe.Option[config]) octobe.Open[postgres, config, Builder] {
	return func() (octobe.Driver[postgres, config, Builder], error) {
		if pool == nil {
			return nil, errors.New("pool is nil")
		}

		var cfg config
		for _, opt := range opts {
			opt(&cfg)
		}

		return &postgres{
			pool: pool,
			cfg:  cfg,
		}, nil
	}
}

// Begin will start a new session with the database, this will return a Session instance that can be used for handling
// queries. Options can be passed to the driver for specific configuration that overwrites the default configuration
// given at instantiation of the Octobe instance. If no options are passed, the default configuration will be used.
// If the default configuration is not set, the session will not be transactional.
func (d *postgres) Begin(ctx context.Context, opts ...octobe.Option[config]) (octobe.Session[Builder], error) {
	cfg := d.cfg
	for _, opt := range opts {
		opt(&cfg)
	}

	var tx pgx.Tx
	var err error
	if cfg.txOptions == nil {
		tx, err = d.pool.Begin(ctx)
	} else {
		tx, err = d.pool.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:       cfg.txOptions.IsoLevel,
			AccessMode:     cfg.txOptions.AccessMode,
			DeferrableMode: cfg.txOptions.DeferrableMode,
			BeginQuery:     cfg.txOptions.BeginQuery,
		})
	}

	if err != nil {
		return nil, err
	}

	return &session{
		ctx: ctx,
		cfg: cfg,
		tx:  tx,
	}, nil
}

// session is a struct that holds session context, a session should be considered a series of queries that are related
// to each other. A session can be transactional or non-transactional, if it is transactional, it will enforce the usage
// of commit and rollback. If it is non-transactional, it will not enforce the usage of commit and rollback.
// A session is not thread safe, it should only be used in one thread at a time.
type session struct {
	ctx       context.Context
	cfg       config
	tx        pgx.Tx
	committed bool
}

// Type check to make sure that the session implements the Octobe Session interface
var _ octobe.Session[Builder] = &session{}

// Commit will commit a transaction, this will only work if the session is transactional.
func (s *session) Commit() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	defer func() {
		s.committed = true
	}()
	return s.tx.Commit(s.ctx)
}

// Rollback will rollback a transaction, this will only work if the session is transactional.
func (s *session) Rollback() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.tx.Rollback(s.ctx)
}

// WatchRollback will watch for a rollback, if the session is not committed, it will rollback the transaction.
func (s *session) WatchRollback(cb func() error) {
	if !s.committed {
		_ = s.Rollback()
		return
	}

	if err := cb(); err != nil {
		_ = s.Rollback()
	}
}

// Builder will return a new builder for building queries
func (s *session) Builder() Builder {
	return s.New
}

// Builder is a function that is used for building queries
type Builder func(query string) Segment

// New will return a new segment for building queries
func (s *session) New(query string) Segment {
	return Segment{
		query: query,
		args:  nil,
		used:  false,
		tx:    s.tx,
		ctx:   s.ctx,
	}
}

// Segment is a specific query that can be run only once it keeps a few fields for keeping track on the Segment
type Segment struct {
	// query in SQL that is going to be executed
	query string
	// args include argument values
	args []interface{}
	// used specify if this Segment already has been executed
	used bool
	// tx is the database transaction, initiated by BeginTx
	tx pgx.Tx
	// ctx is a context that can be used to interrupt a query
	ctx context.Context
}

// use will set used to true after a Segment has been performed
func (s *Segment) use() {
	s.used = true
}

// Arguments receives unknown amount of arguments to use in the query
func (s *Segment) Arguments(args ...interface{}) *Segment {
	s.args = args
	return s
}

// Exec will execute a query. Used for inserts or updates
func (s *Segment) Exec() (pgconn.CommandTag, error) {
	if s.used {
		return pgconn.CommandTag{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	return s.tx.Exec(s.ctx, s.query, s.args...)
}

// QueryRow will return one result and put them into destination pointers
func (s *Segment) QueryRow(dest ...interface{}) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()
	return s.tx.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
}

// Query will perform a normal query against database that returns rows
func (s *Segment) Query(cb func(pgx.Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	rows, err := s.tx.Query(s.ctx, s.query, s.args...)
	if err != nil {
		return err
	}

	defer rows.Close()
	if err = cb(rows); err != nil {
		return err
	}

	return nil
}

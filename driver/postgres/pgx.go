package postgres

import (
	"context"
	"errors"

	"github.com/Kansuler/octobe/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PGXConn defines the essential pgx connection interface for database operations.
type PGXConn interface {
	Close(context.Context) error
	Prepare(context.Context, string, string) (*pgconn.StatementDescription, error)
	Deallocate(context.Context, string) error
	DeallocateAll(context.Context) error
	Ping(context.Context) error
	PgConn() *pgconn.PgConn
	Config() *pgx.ConnConfig
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
	SendBatch(context.Context, *pgx.Batch) pgx.BatchResults
	Begin(context.Context) (pgx.Tx, error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	CopyFrom(context.Context, pgx.Identifier, []string, pgx.CopyFromSource) (int64, error)
}

var _ PGXConn = &pgx.Conn{}

type pgxConn struct {
	conn PGXConn
}

var _ PGXDriver = &pgxConn{}

// OpenPGX creates a pgx connection driver from a DSN string.
func OpenPGX(ctx context.Context, dsn string) octobe.Open[pgxConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxConn, pgxConfig, Builder], error) {
		conn, err := pgx.Connect(ctx, dsn)
		if err != nil {
			return nil, err
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// ParseConfigOptions wraps pgconn parse configuration options.
type ParseConfigOptions struct {
	pgconn.ParseConfigOptions
}

// OpenPGXWithOptions creates a pgx connection driver with custom parse options.
func OpenPGXWithOptions(ctx context.Context, dsn string, options ParseConfigOptions) octobe.Open[pgxConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxConn, pgxConfig, Builder], error) {
		conn, err := pgx.ConnectWithOptions(ctx, dsn, pgx.ParseConfigOptions{ParseConfigOptions: options.ParseConfigOptions})
		if err != nil {
			return nil, err
		}

		return &pgxConn{
			conn: conn,
		}, nil
	}
}

// OpenPGXWithConn creates a driver from an existing pgx connection.
func OpenPGXWithConn(c PGXConn) octobe.Open[pgxConn, pgxConfig, Builder] {
	return func() (octobe.Driver[pgxConn, pgxConfig, Builder], error) {
		if c == nil {
			return nil, errors.New("conn is nil")
		}

		return &pgxConn{
			conn: c,
		}, nil
	}
}

// Begin starts a new session, optionally within a transaction if txOptions are provided.
func (d *pgxConn) Begin(ctx context.Context, opts ...octobe.Option[pgxConfig]) (octobe.Session[Builder], error) {
	var cfg pgxConfig
	for _, opt := range opts {
		opt(&cfg)
	}

	var tx pgx.Tx
	var err error
	if cfg.txOptions != nil {
		tx, err = d.conn.BeginTx(ctx, pgx.TxOptions{
			IsoLevel:       cfg.txOptions.IsoLevel,
			AccessMode:     cfg.txOptions.AccessMode,
			DeferrableMode: cfg.txOptions.DeferrableMode,
			BeginQuery:     cfg.txOptions.BeginQuery,
		})
	}

	if err != nil {
		return nil, err
	}

	return &pgxSession{
		ctx: ctx,
		cfg: cfg,
		tx:  tx,
		d:   d,
	}, nil
}

func (d *pgxConn) Close(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Close(ctx)
}

func (d *pgxConn) Ping(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Ping(ctx)
}

func (d *pgxConn) StartTransaction(ctx context.Context, fn func(session octobe.BuilderSession[Builder]) error, opts ...octobe.Option[pgxConfig]) (err error) {
	return octobe.StartTransaction[pgxConn, pgxConfig, Builder](ctx, d, fn, opts...)
}

// pgxSession manages a database session that may be transactional or non-transactional.
// Not thread-safe - use one session per goroutine.
type pgxSession struct {
	ctx       context.Context
	cfg       pgxConfig
	tx        pgx.Tx
	d         *pgxConn
	committed bool
}

var _ octobe.Session[Builder] = &pgxSession{}

// Commit commits the transaction. Only works for transactional sessions.
func (s *pgxSession) Commit() error {
	if s.committed {
		return errors.New("cannot commit a session that has already been committed")
	}

	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	defer func() {
		s.committed = true
	}()
	return s.tx.Commit(s.ctx)
}

// Rollback rolls back the transaction. Only works for transactional sessions.
func (s *pgxSession) Rollback() error {
	if s.cfg.txOptions == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.tx.Rollback(s.ctx)
}

// Builder returns a query builder function for this session.
func (s *pgxSession) Builder() Builder {
	return func(query string) Segment {
		return &pgxSegment{
			query: query,
			args:  nil,
			used:  false,
			tx:    s.tx,
			d:     s.d,
			ctx:   s.ctx,
		}
	}
}

// pgxSegment represents a single-use query with arguments and execution tracking.
type pgxSegment struct {
	query string
	args  []any
	used  bool
	tx    pgx.Tx
	d     *pgxConn
	ctx   context.Context
}

var _ Segment = &pgxSegment{}

func (s *pgxSegment) use() {
	s.used = true
}

// Arguments sets query parameters and returns the segment for method chaining.
func (s *pgxSegment) Arguments(args ...any) Segment {
	s.args = args
	return s
}

// Exec executes the query and returns the number of affected rows.
func (s *pgxSegment) Exec() (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		res, err := s.d.conn.Exec(s.ctx, s.query, s.args...)
		if err != nil {
			return ExecResult{}, err
		}

		return ExecResult{
			RowsAffected: res.RowsAffected(),
		}, nil
	}

	res, err := s.tx.Exec(s.ctx, s.query, s.args...)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{
		RowsAffected: res.RowsAffected(),
	}, nil
}

// QueryRow executes the query expecting exactly one row and scans into dest.
func (s *pgxSegment) QueryRow(dest ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()
	if s.tx == nil {
		return s.d.conn.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
	}
	return s.tx.QueryRow(s.ctx, s.query, s.args...).Scan(dest...)
}

// Query executes the query and calls cb for each row in the result set.
func (s *pgxSegment) Query(cb func(Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	var err error
	var rows pgx.Rows
	if s.tx == nil {
		rows, err = s.d.conn.Query(s.ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	} else {
		rows, err = s.tx.Query(s.ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	}

	defer rows.Close()
	if err = cb(rows); err != nil {
		return err
	}

	return nil
}

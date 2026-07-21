package postgres

import (
	"context"
	"errors"

	"github.com/Kansuler/octobe/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// PGXConn defines the pgx connection methods used by the driver.
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
func OpenPGX(ctx context.Context, dsn string) PGXOpen {
	return func() (PGXDriver, error) {
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
func OpenPGXWithOptions(ctx context.Context, dsn string, options ParseConfigOptions) PGXOpen {
	return func() (PGXDriver, error) {
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
func OpenPGXWithConn(c PGXConn) PGXOpen {
	return func() (PGXDriver, error) {
		if c == nil {
			return nil, errors.New("conn is nil")
		}

		return &pgxConn{
			conn: c,
		}, nil
	}
}

// Begin starts a non-transactional session on the underlying pgx connection.
func (d *pgxConn) Begin(ctx context.Context) (*octobe.Session[Builder], error) {
	return octobe.NewSession(&pgxSession{
		d: d,
	})
}

// BeginTx starts a new transactional session.
func (d *pgxConn) BeginTx(ctx context.Context, opts ...Option) (*octobe.Session[Builder], error) {
	var cfg Config
	for _, opt := range transactionOptions(opts) {
		opt(&cfg)
	}

	var pgxOpts pgx.TxOptions
	if cfg.txOptions != nil {
		pgxOpts = pgx.TxOptions{
			IsoLevel:       cfg.txOptions.IsoLevel,
			AccessMode:     cfg.txOptions.AccessMode,
			DeferrableMode: cfg.txOptions.DeferrableMode,
			BeginQuery:     cfg.txOptions.BeginQuery,
		}
	}

	tx, err := d.conn.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	session := pgxSession{
		cfg:       cfg,
		tx:        tx,
		committed: false,
		closed:    false,
	}

	return octobe.NewSession(&session)
}

// Close closes the connection.
func (d *pgxConn) Close(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Close(ctx)
}

// Ping pings the connection.
func (d *pgxConn) Ping(ctx context.Context) error {
	if d.conn == nil {
		return errors.New("connection is nil")
	}
	return d.conn.Ping(ctx)
}

// StartTransaction runs fn in a transaction managed by Octobe.
func (d *pgxConn) StartTransaction(ctx context.Context, fn func(session *octobe.ManagedSession[Builder]) error, opts ...Option) (err error) {
	return octobe.StartTransaction[PGXConn](ctx, d, fn, opts...)
}

// pgxSession implements octobe.Backend for a pgx connection or transaction.
// It is not safe for concurrent use.
type pgxSession struct {
	cfg       Config
	tx        pgx.Tx
	d         *pgxConn
	committed bool
	closed    bool
}

var _ octobe.Backend[Builder] = &pgxSession{}

// Commit commits the transaction. Only works for transactional sessions.
func (s *pgxSession) Commit(ctx context.Context) error {
	if s.committed {
		return errors.New("cannot commit a session that has already been committed")
	}

	if s.cfg.txOptions == nil {
		return errors.New("cannot commit without transaction")
	}
	if s.closed {
		return errors.New("cannot commit a session that has already been closed")
	}
	err := s.tx.Commit(ctx)
	s.committed = true
	if err == nil {
		s.closed = true
	}
	return err
}

// Rollback rolls back the transaction. Only works for transactional sessions.
func (s *pgxSession) Rollback(ctx context.Context) error {
	if s.tx == nil {
		return errors.New("cannot rollback without transaction")
	}
	if s.closed {
		return nil
	}
	defer func() {
		s.closed = true
	}()
	return s.tx.Rollback(ctx)
}

// Close closes the session, rolling back if it is transactional and not committed.
func (s *pgxSession) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	if s.cfg.txOptions != nil {
		return s.Rollback(ctx)
	}
	s.closed = true
	return nil
}

// Builder returns a query builder function for this session.
func (s *pgxSession) Builder() Builder {
	return func(query string) Segment {
		return &pgxSegment{
			query:   query,
			args:    nil,
			used:    false,
			session: s,
		}
	}
}

// pgxSegment represents a single-use query with arguments and execution tracking.
type pgxSegment struct {
	query   string
	args    []any
	used    bool
	session *pgxSession
}

var _ Segment = &pgxSegment{}

func (s *pgxSegment) use() {
	s.used = true
}

// activeSession returns the session associated with this segment, or an error if it is closed.
func (s *pgxSegment) activeSession() (*pgxSession, error) {
	if s.session == nil || s.session.closed {
		return nil, errors.New("session is closed")
	}
	return s.session, nil
}

// Arguments sets query parameters and returns the segment for method chaining.
func (s *pgxSegment) Arguments(args ...any) Segment {
	s.args = args
	return s
}

// Exec executes the query and returns the number of affected rows.
func (s *pgxSegment) Exec(ctx context.Context) (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrAlreadyUsed
	}
	defer s.use()
	session, err := s.activeSession()
	if err != nil {
		return ExecResult{}, err
	}
	if session.tx == nil {
		res, err := session.d.conn.Exec(ctx, s.query, s.args...)
		if err != nil {
			return ExecResult{}, err
		}

		return ExecResult{
			RowsAffected: res.RowsAffected(),
		}, nil
	}

	res, err := session.tx.Exec(ctx, s.query, s.args...)
	if err != nil {
		return ExecResult{}, err
	}
	return ExecResult{
		RowsAffected: res.RowsAffected(),
	}, nil
}

// QueryRow executes the query expecting exactly one row and scans into dest.
func (s *pgxSegment) QueryRow(ctx context.Context, dest ...any) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()
	session, err := s.activeSession()
	if err != nil {
		return err
	}
	if session.tx == nil {
		return session.d.conn.QueryRow(ctx, s.query, s.args...).Scan(dest...)
	}
	return session.tx.QueryRow(ctx, s.query, s.args...).Scan(dest...)
}

// Query executes the query and calls cb for each row in the result set.
func (s *pgxSegment) Query(ctx context.Context, cb func(Rows) error) error {
	if s.used {
		return octobe.ErrAlreadyUsed
	}
	defer s.use()

	session, err := s.activeSession()
	if err != nil {
		return err
	}

	var rows pgx.Rows
	if session.tx == nil {
		rows, err = session.d.conn.Query(ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	} else {
		rows, err = session.tx.Query(ctx, s.query, s.args...)
		if err != nil {
			return err
		}
	}

	defer rows.Close()
	if err = cb(rows); err != nil {
		return err
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}

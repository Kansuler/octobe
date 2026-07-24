package pgx

import (
	"context"
	"errors"

	"github.com/Kansuler/octobe/v4"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool defines the pgxpool methods used by the driver.
type Pool interface {
	Close()
	Acquire(ctx context.Context) (c *pgxpool.Conn, err error)
	BeginTx(context.Context, pgx.TxOptions) (pgx.Tx, error)
	Ping(ctx context.Context) error
}

// PoolSessionConn is an acquired pool connection used by a non-transactional session.
type PoolSessionConn interface {
	Release()
	Exec(context.Context, string, ...any) (pgconn.CommandTag, error)
	Query(context.Context, string, ...any) (pgx.Rows, error)
	QueryRow(context.Context, string, ...any) pgx.Row
}

// PoolSessionConnAcquirer customizes how non-transactional sessions acquire a pinned connection.
type PoolSessionConnAcquirer interface {
	AcquireSessionConn(context.Context) (PoolSessionConn, error)
}

var _ Pool = &pgxpool.Pool{}

type poolConn struct {
	pool Pool
}

type poolAcquiredConn struct {
	conn *pgxpool.Conn
}

func (c *poolAcquiredConn) Release() {
	c.conn.Release()
}

func (c *poolAcquiredConn) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	return c.conn.Exec(ctx, query, args...)
}

func (c *poolAcquiredConn) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	return c.conn.Query(ctx, query, args...)
}

func (c *poolAcquiredConn) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return c.conn.QueryRow(ctx, query, args...)
}

var (
	_ PoolDriver      = &poolConn{}
	_ PoolSessionConn = &poolAcquiredConn{}
)

// OpenPool creates a connection pool driver from a DSN and verifies connectivity.
func OpenPool(ctx context.Context, dsn string) PoolOpenFunc {
	return func() (PoolDriver, error) {
		pool, err := pgxpool.New(ctx, dsn)
		if err != nil {
			return nil, err
		}
		if err := pool.Ping(ctx); err != nil {
			pool.Close()
			return nil, err
		}

		return &poolConn{
			pool: pool,
		}, nil
	}
}

// OpenWithPool creates a driver from an existing pool.
func OpenWithPool(pool Pool) PoolOpenFunc {
	return func() (PoolDriver, error) {
		if pool == nil {
			return nil, errors.New("pool is nil")
		}

		return &poolConn{
			pool: pool,
		}, nil
	}
}

// OpenSession opens a non-transactional session that pins one pool connection until Close.
func (d *poolConn) Session(ctx context.Context) (*octobe.Session[QueryFactory], error) {
	conn, err := d.acquireSession(ctx)
	if err != nil {
		return nil, err
	}

	return octobe.NewSession(&poolSession{
		conn: conn,
	})
}

func (d *poolConn) acquireSession(ctx context.Context) (PoolSessionConn, error) {
	if d.pool == nil {
		return nil, errors.New("pool is nil")
	}

	if acquirer, ok := d.pool.(PoolSessionConnAcquirer); ok {
		conn, err := acquirer.AcquireSessionConn(ctx)
		if err != nil {
			return nil, err
		}
		if conn == nil {
			return nil, errors.New("pool acquired nil connection")
		}
		return conn, nil
	}

	conn, err := d.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	if conn == nil {
		return nil, errors.New("pool acquired nil connection")
	}
	return &poolAcquiredConn{conn: conn}, nil
}

// BeginTx starts a new transactional session.
func (d *poolConn) Transaction(ctx context.Context, opts ...Option) (*octobe.SessionTransaction[QueryFactory], error) {
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

	tx, err := d.pool.BeginTx(ctx, pgxOpts)
	if err != nil {
		return nil, err
	}

	return octobe.NewTransaction(&pgxSession{
		cfg:       cfg,
		tx:        tx,
		committed: false,
		closed:    false,
	})
}

// Close releases the pool connection.
func (d *poolConn) Close(_ context.Context) error {
	if d.pool == nil {
		return errors.New("pool is nil")
	}
	d.pool.Close()
	return nil
}

// Ping pings the pool connection.
func (d *poolConn) Ping(ctx context.Context) error {
	if d.pool == nil {
		return errors.New("pool is nil")
	}
	return d.pool.Ping(ctx)
}

// RunInTransaction runs fn in a transaction managed by Octobe.
func (d *poolConn) RunInTransaction(ctx context.Context, fn func(session *octobe.SessionManaged[QueryFactory]) error, opts ...Option) (err error) {
	return octobe.RunInTransaction[Pool](ctx, d, fn, opts...)
}

// poolSession implements octobe.Backend for a pinned pool connection.
type poolSession struct {
	cfg       Config
	tx        pgx.Tx
	conn      PoolSessionConn
	committed bool
	closed    bool
}

var _ octobe.Backend[QueryFactory] = &poolSession{}

// Commit commits the transaction.
func (s *poolSession) Commit(ctx context.Context) error {
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

// Rollback rolls back the transaction.
func (s *poolSession) Rollback(ctx context.Context) error {
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

// Close closes the session, rolling back if necessary.
func (s *poolSession) Close(ctx context.Context) error {
	if s.closed {
		return nil
	}
	if s.tx != nil {
		return s.Rollback(ctx)
	}
	if s.conn != nil {
		s.conn.Release()
		s.conn = nil
	}
	s.closed = true
	return nil
}

// QueryFactory returns a query factory for this session.
func (s *poolSession) QueryFactory() QueryFactory {
	return func(query string) Statement {
		return &poolStatement{
			query:   query,
			args:    nil,
			used:    false,
			session: s,
		}
	}
}

// poolStatement represents a single-use query with arguments.
type poolStatement struct {
	query   string
	args    []any
	used    bool
	session *poolSession
}

var _ Statement = &poolStatement{}

func (s *poolStatement) use() {
	s.used = true
}

// activeSession returns the active session for this statement.
func (s *poolStatement) activeSession() (*poolSession, error) {
	if s.session == nil || s.session.closed {
		return nil, errors.New("session is closed")
	}
	return s.session, nil
}

// WithArgs sets query parameters.
func (s *poolStatement) WithArgs(args ...any) Statement {
	s.args = args
	return s
}

// Exec executes the query and returns affected rows.
func (s *poolStatement) Exec(ctx context.Context) (ExecResult, error) {
	if s.used {
		return ExecResult{}, octobe.ErrStatementAlreadyExecuted
	}
	defer s.use()
	session, err := s.activeSession()
	if err != nil {
		return ExecResult{}, err
	}
	if session.tx == nil {
		if session.conn == nil {
			return ExecResult{}, errors.New("pool session connection is nil")
		}
		res, err := session.conn.Exec(ctx, s.query, s.args...)
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

// QueryRow executes the query expecting one row and scans into dest.
func (s *poolStatement) QueryRow(ctx context.Context, dest ...any) error {
	if s.used {
		return octobe.ErrStatementAlreadyExecuted
	}
	defer s.use()
	session, err := s.activeSession()
	if err != nil {
		return err
	}
	if session.tx == nil {
		if session.conn == nil {
			return errors.New("pool session connection is nil")
		}
		return session.conn.QueryRow(ctx, s.query, s.args...).Scan(dest...)
	}
	return session.tx.QueryRow(ctx, s.query, s.args...).Scan(dest...)
}

// Query executes the query and passes the result set to handleRows.
func (s *poolStatement) Query(ctx context.Context, handleRows func(Rows) error) error {
	if s.used {
		return octobe.ErrStatementAlreadyExecuted
	}
	defer s.use()

	session, err := s.activeSession()
	if err != nil {
		return err
	}

	var rows pgx.Rows
	if session.tx == nil {
		if session.conn == nil {
			return errors.New("pool session connection is nil")
		}
		rows, err = session.conn.Query(ctx, s.query, s.args...)
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
	if err = handleRows(rows); err != nil {
		return err
	}

	if err = rows.Err(); err != nil {
		return err
	}

	return nil
}

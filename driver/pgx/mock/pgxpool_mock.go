package mock

import (
	"context"
	"errors"
	"fmt"
	"sync"

	opgx "github.com/Kansuler/octobe/v4/driver/pgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// Pool provides a mock implementation of opgx.Pool and pgx.Tx interfaces
// for testing database pool interactions without requiring an actual database connection.
type Pool struct {
	mu              sync.Mutex
	expectations    []expectation
	unexpectedCalls []error
}

var (
	_ opgx.Pool                    = (*Pool)(nil)
	_ opgx.PoolSessionConnAcquirer = (*Pool)(nil)
	_ opgx.PoolSessionConn         = (*Pool)(nil)
	_ pgx.Tx                       = (*Pool)(nil)
)

// NewPool creates a new mock database connection pool for testing.
func NewPool() *Pool {
	return &Pool{}
}

// findExpectation locates the first unfulfilled expectation matching the method and arguments.
func (m *Pool) findExpectation(method string, args ...any) (expectation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, e := range m.expectations {
		if e.fulfilled() {
			continue
		}
		if err := e.match(method, args...); err != nil {
			return nil, fmt.Errorf("%w: next expectation %s does not match %s with args %v: %w", ErrNoExpectation, e, method, args, err)
		}
		return e, nil
	}

	return nil, fmt.Errorf("%w for %s with args %v", ErrNoExpectation, method, args)
}

func (m *Pool) recordUnexpectedCall(err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.unexpectedCalls = append(m.unexpectedCalls, err)
}

// AllExpectationsMet verifies that all configured expectations have been fulfilled.
func (m *Pool) AllExpectationsMet() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.unexpectedCalls) > 0 {
		return errors.Join(m.unexpectedCalls...)
	}
	for _, e := range m.expectations {
		if !e.fulfilled() {
			return fmt.Errorf("unfulfilled expectation: %s", e)
		}
	}
	return nil
}

func (m *Pool) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Ping(ctx context.Context) error {
	e, err := m.findExpectation("Ping")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

func (m *Pool) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Close() {
	e, err := m.findExpectation("Close")
	if err != nil {
		m.recordUnexpectedCall(fmt.Errorf("unexpected Close: %w", err))
		return
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return
	}
}

type ReleaseExpectation struct{ basicExpectation }

func (m *Pool) ExpectRelease() *ReleaseExpectation {
	e := &ReleaseExpectation{basicExpectation: basicExpectation{method: "Release"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Release() {
	e, err := m.findExpectation("Release")
	if err != nil {
		m.recordUnexpectedCall(fmt.Errorf("unexpected Release: %w", err))
		return
	}
	e.getReturns()
}

// ExpectExec configures an expectation for an Exec operation with the specified query.
func (m *Pool) ExpectExec(query string) *ExecExpectation {
	e := &ExecExpectation{
		basicExpectation: basicExpectation{
			method:     "Exec",
			query:      query,
			queryMatch: queryMatchExact,
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	e, err := m.findExpectation("Exec", append([]any{query}, args...)...)
	if err != nil {
		return pgconn.CommandTag{}, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return pgconn.CommandTag{}, ret[1].(error)
	}
	return ret[0].(pgconn.CommandTag), nil
}

// ExpectQuery configures an expectation for a Query operation with the specified query.
func (m *Pool) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method:     "Query",
			query:      query,
			queryMatch: queryMatchExact,
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	e, err := m.findExpectation("Query", append([]any{query}, args...)...)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}
	if ret[0] == nil {
		return nil, nil
	}
	return ret[0].(pgx.Rows), nil
}

// ExpectQueryRow configures an expectation for a QueryRow operation with the specified query.
func (m *Pool) ExpectQueryRow(query string) *QueryRowExpectation {
	e := &QueryRowExpectation{
		basicExpectation: basicExpectation{
			method:     "QueryRow",
			query:      query,
			queryMatch: queryMatchExact,
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &Row{err: err}
	}
	ret := e.getReturns()
	return ret[0].(pgx.Row)
}

func (m *Pool) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Begin(ctx context.Context) (pgx.Tx, error) {
	e, err := m.findExpectation("Begin")
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return nil, ret[1].(error)
	}
	return m, nil
}

func (m *Pool) ExpectBeginTx() *BeginTxExpectation {
	e := &BeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
	e, err := m.findExpectation("BeginTx", txOptions)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return nil, ret[1].(error)
	}
	return m, nil
}

func (m *Pool) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Commit(ctx context.Context) error {
	e, err := m.findExpectation("Commit")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

func (m *Pool) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Rollback(ctx context.Context) error {
	e, err := m.findExpectation("Rollback")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

type AcquireExpectation struct {
	basicExpectation
}

func (e *AcquireExpectation) WillReturnConn(conn *pgxpool.Conn) {
	e.returns = []any{conn, nil}
}

func (e *AcquireExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

// ExpectAcquire configures an expectation for acquiring a connection from the pool.
func (m *Pool) ExpectAcquire() *AcquireExpectation {
	e := &AcquireExpectation{basicExpectation: basicExpectation{method: "Acquire", returns: []any{nil, nil}}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
	e, err := m.findExpectation("Acquire")
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}
	if ret[0] == nil {
		return nil, nil
	}
	return ret[0].(*pgxpool.Conn), nil
}

func (m *Pool) AcquireSessionConn(ctx context.Context) (opgx.PoolSessionConn, error) {
	e, err := m.findExpectation("Acquire")
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if ret[1] != nil {
		return nil, ret[1].(error)
	}
	if ret[0] == nil {
		return m, nil
	}
	conn, ok := ret[0].(opgx.PoolSessionConn)
	if !ok {
		return nil, fmt.Errorf("acquired connection does not implement pgx.PoolSessionConn")
	}
	return conn, nil
}

type AcquireFuncExpectation struct {
	basicExpectation
}

func (e *AcquireFuncExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

// ExpectAcquireFunc configures an expectation for AcquireFunc operations.
func (m *Pool) ExpectAcquireFunc() *AcquireFuncExpectation {
	e := &AcquireFuncExpectation{basicExpectation: basicExpectation{method: "AcquireFunc"}}
	m.expectations = append(m.expectations, e)
	return e
}

// AcquireFunc executes fn with a nil connection for mock purposes.
func (m *Pool) AcquireFunc(ctx context.Context, fn func(*pgxpool.Conn) error) error {
	e, err := m.findExpectation("AcquireFunc")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return fn(nil)
}

type AcquireAllIdleExpectation struct {
	basicExpectation
}

func (e *AcquireAllIdleExpectation) WillReturnConns(conns []*pgxpool.Conn) {
	e.returns = []any{conns}
}

// ExpectAcquireAllIdle configures an expectation for acquiring all idle connections.
func (m *Pool) ExpectAcquireAllIdle() *AcquireAllIdleExpectation {
	e := &AcquireAllIdleExpectation{basicExpectation: basicExpectation{method: "AcquireAllIdle"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
	e, err := m.findExpectation("AcquireAllIdle")
	if err != nil {
		return nil
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].([]*pgxpool.Conn)
	}
	return nil
}

// ExpectPrepare configures an expectation for preparing a statement.
func (m *Pool) ExpectPrepare(name, sql string) *PrepareExpectation {
	e := &PrepareExpectation{
		basicExpectation: basicExpectation{
			method: "Prepare",
			args:   []any{name, sql},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
	e, err := m.findExpectation("Prepare", name, sql)
	if err != nil {
		return nil, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return nil, ret[1].(error)
	}
	if len(ret) > 0 && ret[0] == nil {
		return &pgconn.StatementDescription{Name: name, SQL: sql}, nil
	}
	if len(ret) > 0 {
		return ret[0].(*pgconn.StatementDescription), nil
	}
	return &pgconn.StatementDescription{Name: name, SQL: sql}, nil
}

// ExpectCopyFrom configures an expectation for bulk copy operations.
func (m *Pool) ExpectCopyFrom(tableName pgx.Identifier) *CopyFromExpectation {
	e := &CopyFromExpectation{
		basicExpectation: basicExpectation{
			method: "CopyFrom",
			args:   []any{tableName},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Pool) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
	e, err := m.findExpectation("CopyFrom", tableName, columnNames)
	if err != nil {
		return 0, err
	}
	ret := e.getReturns()
	if len(ret) > 1 && ret[1] != nil {
		return 0, ret[1].(error)
	}
	if len(ret) > 0 {
		return ret[0].(int64), nil
	}
	return 0, nil
}

// Methods that return nil/defaults for interface compliance
func (m *Pool) Reset()                  {}
func (m *Pool) Config() *pgxpool.Config { return nil }
func (m *Pool) Stat() *pgxpool.Stat     { return nil }
func (m *Pool) LargeObjects() pgx.LargeObjects {
	panic("not implemented")
}
func (m *Pool) Conn() *pgx.Conn { return nil }

func (m *Pool) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return nil
}

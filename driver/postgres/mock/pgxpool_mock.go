package mock

import (
	"context"
	"fmt"
	"regexp"
	"sync"

	"github.com/Kansuler/octobe/v3/driver/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
)

// PGXPoolMock provides a mock implementation of postgres.PGXPool and pgx.Tx interfaces
// for testing database pool interactions without requiring an actual database connection.
type PGXPoolMock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var (
	_ postgres.PGXPool = (*PGXPoolMock)(nil)
	_ pgx.Tx           = (*PGXPoolMock)(nil)
)

// NewPGXPoolMock creates a new mock database connection pool for testing.
func NewPGXPoolMock() *PGXPoolMock {
	return &PGXPoolMock{}
}

// findExpectation locates the first unfulfilled expectation matching the method and arguments.
func (m *PGXPoolMock) findExpectation(method string, args ...any) (expectation, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for _, e := range m.expectations {
		if e.fulfilled() {
			continue
		}
		if err := e.match(method, args...); err == nil {
			return e, nil
		}
	}

	return nil, fmt.Errorf("%w for %s with args %v", ErrNoExpectation, method, args)
}

// AllExpectationsMet verifies that all configured expectations have been fulfilled.
func (m *PGXPoolMock) AllExpectationsMet() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.expectations {
		if !e.fulfilled() {
			return fmt.Errorf("unfulfilled expectation: %s", e)
		}
	}
	return nil
}

func (m *PGXPoolMock) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Ping(ctx context.Context) error {
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

func (m *PGXPoolMock) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Close() {
	e, err := m.findExpectation("Close")
	if err != nil {
		return
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return
	}
	return
}

// ExpectExec configures an expectation for an Exec operation with the specified query.
func (m *PGXPoolMock) ExpectExec(query string) *ExecExpectation {
	e := &ExecExpectation{
		basicExpectation: basicExpectation{
			method: "Exec",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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
func (m *PGXPoolMock) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method: "Query",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
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
func (m *PGXPoolMock) ExpectQueryRow(query string) *QueryRowExpectation {
	e := &QueryRowExpectation{
		basicExpectation: basicExpectation{
			method: "QueryRow",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &Row{err: err}
	}
	ret := e.getReturns()
	return ret[0].(pgx.Row)
}

func (m *PGXPoolMock) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Begin(ctx context.Context) (pgx.Tx, error) {
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

func (m *PGXPoolMock) ExpectBeginTx() *BeginTxExpectation {
	e := &BeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
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

func (m *PGXPoolMock) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Commit(ctx context.Context) error {
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

func (m *PGXPoolMock) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Rollback(ctx context.Context) error {
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
func (m *PGXPoolMock) ExpectAcquire() *AcquireExpectation {
	e := &AcquireExpectation{basicExpectation: basicExpectation{method: "Acquire"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Acquire(ctx context.Context) (*pgxpool.Conn, error) {
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

type AcquireFuncExpectation struct {
	basicExpectation
}

func (e *AcquireFuncExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

// ExpectAcquireFunc configures an expectation for AcquireFunc operations.
func (m *PGXPoolMock) ExpectAcquireFunc() *AcquireFuncExpectation {
	e := &AcquireFuncExpectation{basicExpectation: basicExpectation{method: "AcquireFunc"}}
	m.expectations = append(m.expectations, e)
	return e
}

// AcquireFunc executes fn with a nil connection for mock purposes.
func (m *PGXPoolMock) AcquireFunc(ctx context.Context, fn func(*pgxpool.Conn) error) error {
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
func (m *PGXPoolMock) ExpectAcquireAllIdle() *AcquireAllIdleExpectation {
	e := &AcquireAllIdleExpectation{basicExpectation: basicExpectation{method: "AcquireAllIdle"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) AcquireAllIdle(ctx context.Context) []*pgxpool.Conn {
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

type PoolPrepareExpectation struct {
	basicExpectation
}

func (e *PoolPrepareExpectation) WithName(name string) *PoolPrepareExpectation {
	e.args = []any{name}
	return e
}

func (e *PoolPrepareExpectation) WillReturnResult(desc *pgconn.StatementDescription) {
	e.returns = []any{desc, nil}
}

func (e *PoolPrepareExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

// ExpectPrepare configures an expectation for preparing a statement.
func (m *PGXPoolMock) ExpectPrepare(name, sql string) *PoolPrepareExpectation {
	e := &PoolPrepareExpectation{
		basicExpectation: basicExpectation{
			method: "Prepare",
			args:   []any{name, sql},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
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

type PoolCopyFromExpectation struct {
	basicExpectation
}

func (e *PoolCopyFromExpectation) WithColumns(columns []string) *PoolCopyFromExpectation {
	e.args = append(e.args, columns)
	return e
}

func (e *PoolCopyFromExpectation) WillReturnResult(rowsAffected int64) {
	e.returns = []any{rowsAffected, nil}
}

func (e *PoolCopyFromExpectation) WillReturnError(err error) {
	e.returns = []any{int64(0), err}
}

// ExpectCopyFrom configures an expectation for bulk copy operations.
func (m *PGXPoolMock) ExpectCopyFrom(tableName pgx.Identifier) *PoolCopyFromExpectation {
	e := &PoolCopyFromExpectation{
		basicExpectation: basicExpectation{
			method: "CopyFrom",
			args:   []any{tableName},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXPoolMock) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
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
func (m *PGXPoolMock) Reset()                  {}
func (m *PGXPoolMock) Config() *pgxpool.Config { return nil }
func (m *PGXPoolMock) Stat() *pgxpool.Stat     { return nil }
func (m *PGXPoolMock) LargeObjects() pgx.LargeObjects {
	panic("not implemented")
}
func (m *PGXPoolMock) Conn() *pgx.Conn { return nil }

func (m *PGXPoolMock) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return nil
}

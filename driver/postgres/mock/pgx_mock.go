package mock

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"

	"github.com/Kansuler/octobe/v3/driver/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrNoExpectation = errors.New("no expectation found")

// PGXMock provides a mock implementation of postgres.PGXConn and pgx.Tx interfaces
// for testing database interactions without requiring an actual database connection.
type PGXMock struct {
	mu           sync.Mutex
	expectations []expectation
	ordered      bool
}

var (
	_ postgres.PGXConn = (*PGXMock)(nil)
	_ pgx.Tx           = (*PGXMock)(nil)
)

// NewPGXMock creates a new mock database connection for testing.
func NewPGXMock() *PGXMock {
	return &PGXMock{}
}

// findExpectation locates the first unfulfilled expectation matching the method and arguments.
func (m *PGXMock) findExpectation(method string, args ...any) (expectation, error) {
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
func (m *PGXMock) AllExpectationsMet() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.expectations {
		if !e.fulfilled() {
			return fmt.Errorf("unfulfilled expectation: %s", e)
		}
	}
	return nil
}

func (m *PGXMock) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Ping(ctx context.Context) error {
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

func (m *PGXMock) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Close(ctx context.Context) error {
	e, err := m.findExpectation("Close")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

// ExpectExec configures an expectation for an Exec operation with the specified query.
func (m *PGXMock) ExpectExec(query string) *ExecExpectation {
	e := &ExecExpectation{
		basicExpectation: basicExpectation{
			method: "Exec",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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
func (m *PGXMock) ExpectQuery(query string) *QueryExpectation {
	e := &QueryExpectation{
		basicExpectation: basicExpectation{
			method: "Query",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
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
func (m *PGXMock) ExpectQueryRow(query string) *QueryRowExpectation {
	e := &QueryRowExpectation{
		basicExpectation: basicExpectation{
			method: "QueryRow",
			query:  regexp.MustCompile(regexp.QuoteMeta(query)),
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &Row{err: err}
	}
	ret := e.getReturns()
	return ret[0].(pgx.Row)
}

func (m *PGXMock) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Begin(ctx context.Context) (pgx.Tx, error) {
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

func (m *PGXMock) ExpectBeginTx() *BeginTxExpectation {
	e := &BeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
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

func (m *PGXMock) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Commit(ctx context.Context) error {
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

func (m *PGXMock) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Rollback(ctx context.Context) error {
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

type PrepareExpectation struct {
	basicExpectation
}

func (e *PrepareExpectation) WithName(name string) *PrepareExpectation {
	e.args = []any{name}
	return e
}

func (e *PrepareExpectation) WillReturnResult(desc *pgconn.StatementDescription) {
	e.returns = []any{desc, nil}
}

func (e *PrepareExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

// ExpectPrepare configures an expectation for preparing a statement.
func (m *PGXMock) ExpectPrepare(name, sql string) *PrepareExpectation {
	e := &PrepareExpectation{
		basicExpectation: basicExpectation{
			method: "Prepare",
			args:   []any{name, sql},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
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

type DeallocateExpectation struct {
	basicExpectation
}

func (e *DeallocateExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

func (m *PGXMock) ExpectDeallocate(name string) *DeallocateExpectation {
	e := &DeallocateExpectation{
		basicExpectation: basicExpectation{
			method: "Deallocate",
			args:   []any{name},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) Deallocate(ctx context.Context, name string) error {
	e, err := m.findExpectation("Deallocate", name)
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

type DeallocateAllExpectation struct {
	basicExpectation
}

func (e *DeallocateAllExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

func (m *PGXMock) ExpectDeallocateAll() *DeallocateAllExpectation {
	e := &DeallocateAllExpectation{
		basicExpectation: basicExpectation{method: "DeallocateAll"},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) DeallocateAll(ctx context.Context) error {
	e, err := m.findExpectation("DeallocateAll")
	if err != nil {
		return err
	}
	ret := e.getReturns()
	if len(ret) > 0 && ret[0] != nil {
		return ret[0].(error)
	}
	return nil
}

type CopyFromExpectation struct {
	basicExpectation
}

func (e *CopyFromExpectation) WithColumns(columns []string) *CopyFromExpectation {
	e.args = append(e.args, columns)
	return e
}

func (e *CopyFromExpectation) WillReturnResult(rowsAffected int64) {
	e.returns = []any{rowsAffected, nil}
}

func (e *CopyFromExpectation) WillReturnError(err error) {
	e.returns = []any{int64(0), err}
}

// ExpectCopyFrom configures an expectation for bulk copy operations.
func (m *PGXMock) ExpectCopyFrom(tableName pgx.Identifier) *CopyFromExpectation {
	e := &CopyFromExpectation{
		basicExpectation: basicExpectation{
			method: "CopyFrom",
			args:   []any{tableName},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *PGXMock) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
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
func (m *PGXMock) PgConn() *pgconn.PgConn  { return nil }
func (m *PGXMock) Config() *pgx.ConnConfig { return nil }
func (m *PGXMock) LargeObjects() pgx.LargeObjects {
	panic("not implemented")
}
func (m *PGXMock) Conn() *pgx.Conn { return nil }

func (m *PGXMock) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return nil
}

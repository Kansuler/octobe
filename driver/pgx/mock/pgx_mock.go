package mock

import (
	"context"
	"errors"
	"fmt"
	"sync"

	opgx "github.com/Kansuler/octobe/v4/driver/pgx"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

var ErrNoExpectation = errors.New("no expectation found")

// Conn provides a mock implementation of opgx.Conn and pgx.Tx interfaces
// for testing database interactions without requiring an actual database connection.
type Conn struct {
	mu           sync.Mutex
	expectations []expectation
}

var (
	_ opgx.Conn = (*Conn)(nil)
	_ pgx.Tx    = (*Conn)(nil)
)

// NewConn creates a new mock database connection for testing.
func NewConn() *Conn {
	return &Conn{}
}

// findExpectation locates the first unfulfilled expectation matching the method and arguments.
func (m *Conn) findExpectation(method string, args ...any) (expectation, error) {
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

// AllExpectationsMet verifies that all configured expectations have been fulfilled.
func (m *Conn) AllExpectationsMet() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, e := range m.expectations {
		if !e.fulfilled() {
			return fmt.Errorf("unfulfilled expectation: %s", e)
		}
	}
	return nil
}

func (m *Conn) ExpectPing() *PingExpectation {
	e := &PingExpectation{basicExpectation: basicExpectation{method: "Ping"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Ping(ctx context.Context) error {
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

func (m *Conn) ExpectClose() *CloseExpectation {
	e := &CloseExpectation{basicExpectation: basicExpectation{method: "Close"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Close(ctx context.Context) error {
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
func (m *Conn) ExpectExec(query string) *ExecExpectation {
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

func (m *Conn) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
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
func (m *Conn) ExpectQuery(query string) *QueryExpectation {
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

func (m *Conn) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
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
func (m *Conn) ExpectQueryRow(query string) *QueryRowExpectation {
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

func (m *Conn) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	e, err := m.findExpectation("QueryRow", append([]any{query}, args...)...)
	if err != nil {
		return &Row{err: err}
	}
	ret := e.getReturns()
	return ret[0].(pgx.Row)
}

func (m *Conn) ExpectBegin() *BeginExpectation {
	e := &BeginExpectation{basicExpectation: basicExpectation{method: "Begin"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Begin(ctx context.Context) (pgx.Tx, error) {
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

func (m *Conn) ExpectBeginTx() *BeginTxExpectation {
	e := &BeginTxExpectation{basicExpectation: basicExpectation{method: "BeginTx"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error) {
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

func (m *Conn) ExpectCommit() *CommitExpectation {
	e := &CommitExpectation{basicExpectation: basicExpectation{method: "Commit"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Commit(ctx context.Context) error {
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

func (m *Conn) ExpectRollback() *RollbackExpectation {
	e := &RollbackExpectation{basicExpectation: basicExpectation{method: "Rollback"}}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Rollback(ctx context.Context) error {
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
func (m *Conn) ExpectPrepare(name, sql string) *PrepareExpectation {
	e := &PrepareExpectation{
		basicExpectation: basicExpectation{
			method: "Prepare",
			args:   []any{name, sql},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Prepare(ctx context.Context, name, sql string) (*pgconn.StatementDescription, error) {
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

func (m *Conn) ExpectDeallocate(name string) *DeallocateExpectation {
	e := &DeallocateExpectation{
		basicExpectation: basicExpectation{
			method: "Deallocate",
			args:   []any{name},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) Deallocate(ctx context.Context, name string) error {
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

func (m *Conn) ExpectDeallocateAll() *DeallocateAllExpectation {
	e := &DeallocateAllExpectation{
		basicExpectation: basicExpectation{method: "DeallocateAll"},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) DeallocateAll(ctx context.Context) error {
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
func (m *Conn) ExpectCopyFrom(tableName pgx.Identifier) *CopyFromExpectation {
	e := &CopyFromExpectation{
		basicExpectation: basicExpectation{
			method: "CopyFrom",
			args:   []any{tableName},
		},
	}
	m.expectations = append(m.expectations, e)
	return e
}

func (m *Conn) CopyFrom(ctx context.Context, tableName pgx.Identifier, columnNames []string, rowSrc pgx.CopyFromSource) (int64, error) {
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
func (m *Conn) PgConn() *pgconn.PgConn  { return nil }
func (m *Conn) Config() *pgx.ConnConfig { return nil }
func (m *Conn) LargeObjects() pgx.LargeObjects {
	panic("not implemented")
}
func (m *Conn) Conn() *pgx.Conn { return nil }

func (m *Conn) SendBatch(ctx context.Context, batch *pgx.Batch) pgx.BatchResults {
	return nil
}

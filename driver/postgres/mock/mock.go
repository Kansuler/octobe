package mock

import (
	"errors"
	"fmt"
	"io"
	"reflect"
	"regexp"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// expectation defines the interface for mock database operation expectations.
type expectation interface {
	fulfilled() bool
	match(method string, args ...any) error
	getReturns() []any
	String() string
}

type basicExpectation struct {
	method      string
	isFulfilled bool
	returns     []any
	query       *regexp.Regexp
	args        []any
}

func (e *basicExpectation) fulfilled() bool {
	return e.isFulfilled
}

func (e *basicExpectation) getReturns() []any {
	e.isFulfilled = true
	return e.returns
}

func (e *basicExpectation) WithArgs(args ...any) {
	e.args = args
}

// match validates that the method call matches the expected signature and arguments.
func (e *basicExpectation) match(method string, args ...any) error {
	if e.method != method {
		return fmt.Errorf("method mismatch: expected %s, got %s", e.method, method)
	}

	if e.query != nil {
		query, ok := args[0].(string)
		if !ok {
			return fmt.Errorf("first argument was not a string query")
		}
		if !e.query.MatchString(query) {
			return fmt.Errorf("query does not match regexp %s", e.query)
		}
		args = args[1:]
	}

	if e.args != nil {
		if !reflect.DeepEqual(e.args, args) {
			return fmt.Errorf("args mismatch: expected %v, got %v", e.args, args)
		}
	}

	return nil
}

func (e *basicExpectation) String() string {
	var queryStr string
	if e.query != nil {
		queryStr = e.query.String()
	} else {
		queryStr = "<nil>"
	}
	return fmt.Sprintf("method %s with query %s and args %v", e.method, queryStr, e.args)
}

type PingExpectation struct {
	basicExpectation
}

func (e *PingExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

type CloseExpectation struct {
	basicExpectation
}

func (e *CloseExpectation) WillReturnError(err error) {
	e.returns = []any{err}
}

// NewResult creates a pgconn.CommandTag for mocking Exec operation results.
func NewResult(command string, rowsAffected int64) pgconn.CommandTag {
	return pgconn.NewCommandTag(fmt.Sprintf("%s 0 %d", command, rowsAffected))
}

type ExecExpectation struct {
	basicExpectation
}

func (e *ExecExpectation) WithArgs(args ...any) *ExecExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *ExecExpectation) WillReturnResult(res pgconn.CommandTag) {
	e.returns = []any{res, nil}
}

func (e *ExecExpectation) WillReturnError(err error) {
	e.returns = []any{pgconn.CommandTag{}, err}
}

type QueryExpectation struct {
	basicExpectation
}

func (e *QueryExpectation) WithArgs(args ...any) *QueryExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *QueryExpectation) WillReturnRows(rows pgx.Rows) {
	e.returns = []any{rows, nil}
}

func (e *QueryExpectation) WillReturnError(err error) {
	e.returns = []any{nil, err}
}

type QueryRowExpectation struct {
	basicExpectation
}

func (e *QueryRowExpectation) WithArgs(args ...any) *QueryRowExpectation {
	e.basicExpectation.WithArgs(args...)
	return e
}

func (e *QueryRowExpectation) WillReturnRow(row pgx.Row) {
	e.returns = []any{row}
}

type BeginExpectation struct{ basicExpectation }

func (e *BeginExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

type BeginTxExpectation struct{ basicExpectation }

func (e *BeginTxExpectation) WithOptions(opts pgx.TxOptions) *BeginTxExpectation {
	e.args = []any{opts}
	return e
}

func (e *BeginTxExpectation) WillReturnError(err error) { e.returns = []any{nil, err} }

type CommitExpectation struct{ basicExpectation }

func (e *CommitExpectation) WillReturnError(err error) { e.returns = []any{err} }

type RollbackExpectation struct{ basicExpectation }

func (e *RollbackExpectation) WillReturnError(err error) { e.returns = []any{err} }

// Row provides a mock implementation of pgx.Row for testing QueryRow operations.
type Row struct {
	row []any
	err error
}

func NewRow(row ...any) *Row {
	return &Row{row: row}
}

func (r *Row) WillReturnError(err error) *Row {
	r.err = err
	return r
}

// Scan copies row values into destination pointers using reflection.
func (r *Row) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	for i, val := range r.row {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(val))
	}
	return nil
}

// Rows provides a mock implementation of pgx.Rows for testing Query operations.
// Supports adding rows and controlling iteration behavior.
type Rows struct {
	fields []pgconn.FieldDescription
	rows   [][]any
	pos    int
	err    error
	closed bool
}

func NewRows(columns []string) *Rows {
	fields := make([]pgconn.FieldDescription, len(columns))
	for i, col := range columns {
		fields[i] = pgconn.FieldDescription{Name: col}
	}
	return &Rows{fields: fields, pos: -1}
}

// AddRow appends a data row with values matching the column count.
func (r *Rows) AddRow(values ...any) *Rows {
	if len(values) != len(r.fields) {
		panic("number of values does not match number of columns")
	}
	r.rows = append(r.rows, values)
	return r
}

func (r *Rows) Close() { r.closed = true }

func (r *Rows) Err() error { return r.err }

func (r *Rows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }

func (r *Rows) FieldDescriptions() []pgconn.FieldDescription { return r.fields }

func (r *Rows) Next() bool {
	if r.closed {
		return false
	}
	r.pos++
	return r.pos < len(r.rows)
}

// Scan copies current row values into destination pointers using reflection.
func (r *Rows) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if r.closed {
		return errors.New("rows is closed")
	}
	if r.pos < 0 || r.pos >= len(r.rows) {
		return io.EOF
	}
	for i, val := range r.rows[r.pos] {
		reflect.ValueOf(dest[i]).Elem().Set(reflect.ValueOf(val))
	}
	return nil
}

func (r *Rows) Values() ([]any, error) {
	if r.pos < 0 || r.pos >= len(r.rows) {
		return nil, io.EOF
	}
	return r.rows[r.pos], nil
}

// RawValues returns current row values as byte slices for compatibility.
func (r *Rows) RawValues() [][]byte {
	if r.pos < 0 || r.pos >= len(r.rows) {
		return nil
	}

	rawValues := make([][]byte, len(r.rows[r.pos]))
	for i, val := range r.rows[r.pos] {
		if val == nil {
			rawValues[i] = nil
		} else {
			rawValues[i] = []byte(fmt.Sprintf("%v", val))
		}
	}
	return rawValues
}

func (r *Rows) Conn() *pgx.Conn { return nil }

// GetRowsForTesting exposes internal row data for test verification.
func (r *Rows) GetRowsForTesting() [][]any {
	return r.rows
}

package mock

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v3"
	"github.com/Kansuler/octobe/v3/driver/postgres"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
)

func TestPoolMock(t *testing.T) {
	ctx := context.Background()

	t.Run("Ping success", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		mock.ExpectPing()
		err = o.Ping(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Ping error", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		expectedErr := errors.New("ping failed")
		mock.ExpectPing().WillReturnError(expectedErr)

		err = o.Ping(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Close success", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		mock.ExpectClose()
		err = o.Close(ctx)
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Exec success without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "INSERT INTO events"
		args := []any{1, "test"}
		mock.ExpectExec(query).WithArgs(args...).WillReturnResult(pgconn.CommandTag{})

		_, err = session.Builder()(query).Arguments(args...).Exec()
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Exec error without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "INSERT INTO events"
		expectedErr := errors.New("exec error")
		mock.ExpectExec(query).WillReturnError(expectedErr)

		_, err = session.Builder()(query).Exec()
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Query success without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT id, name FROM users"
		rows := NewRows([]string{"id", "name"}).
			AddRow(1, "John Doe").
			AddRow(2, "Jane Doe")

		mock.ExpectQuery(query).WillReturnRows(rows)

		err = session.Builder()(query).Query(func(r postgres.Rows) error {
			i := 0
			for r.Next() {
				var id int
				var name string
				require.NoError(t, r.Scan(&id, &name))
				require.Equal(t, rows.GetRowsForTesting()[i][0], id)
				require.Equal(t, rows.GetRowsForTesting()[i][1], name)
				i++
			}
			return r.Err()
		})
		require.NoError(t, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Query error without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT id, name FROM users"
		expectedErr := errors.New("query error")
		mock.ExpectQuery(query).WillReturnError(expectedErr)

		err = session.Builder()(query).Query(func(r postgres.Rows) error {
			return nil
		})
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("QueryRow success without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT name FROM users WHERE id = ?"
		row := NewRow("John Doe")
		mock.ExpectQueryRow(query).WithArgs(1).WillReturnRow(row)

		var name string
		err = session.Builder()(query).Arguments(1).QueryRow(&name)
		require.NoError(t, err)
		require.Equal(t, "John Doe", name)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("QueryRow error without tx", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)
		session, err := o.Begin(ctx)
		require.NoError(t, err)

		query := "SELECT name FROM users WHERE id = ?"
		expectedErr := errors.New("row scan error")
		row := NewRow().WillReturnError(expectedErr)
		mock.ExpectQueryRow(query).WithArgs(1).WillReturnRow(row)

		var name string
		err = session.Builder()(query).Arguments(1).QueryRow(&name)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Transaction success", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		txOpts := postgres.PGXTxOptions{}
		mock.ExpectBeginTx()
		mock.ExpectCommit()

		session, err := o.Begin(ctx, postgres.WithPGXTxOptions(txOpts))
		require.NoError(t, err)

		err = session.Commit()
		require.NoError(t, err)

		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Transaction with exec", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		txOpts := postgres.PGXTxOptions{}
		mock.ExpectBeginTx()
		query := "INSERT INTO users (name) VALUES ($1)"
		mock.ExpectExec(query).WithArgs("test-user").WillReturnResult(pgconn.CommandTag{})
		mock.ExpectCommit()

		session, err := o.Begin(ctx, postgres.WithPGXTxOptions(txOpts))
		require.NoError(t, err)

		_, err = session.Builder()(query).Arguments("test-user").Exec()
		require.NoError(t, err)

		err = session.Commit()
		require.NoError(t, err)

		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Transaction rollback", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		txOpts := postgres.PGXTxOptions{}
		mock.ExpectBeginTx()
		mock.ExpectRollback()

		session, err := o.Begin(ctx, postgres.WithPGXTxOptions(txOpts))
		require.NoError(t, err)

		err = session.Rollback()
		require.NoError(t, err)

		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Unfulfilled expectations", func(t *testing.T) {
		mock := NewPGXPoolMock()
		mock.ExpectPing()
		mock.ExpectClose()

		err := mock.AllExpectationsMet()
		require.Error(t, err)
		require.Contains(t, err.Error(), "unfulfilled expectation: method Ping")
	})

	t.Run("No more expectations", func(t *testing.T) {
		mock := NewPGXPoolMock()
		o, err := octobe.New(postgres.OpenPGXWithPool(mock))
		require.NoError(t, err)

		err = o.Ping(ctx)
		require.Error(t, err)
		require.ErrorIs(t, err, ErrNoExpectation)
	})

	t.Run("Acquire success", func(t *testing.T) {
		mock := NewPGXPoolMock()

		mock.ExpectAcquire().WillReturnConn(nil) // Using nil for simplicity in tests

		conn, err := mock.Acquire(ctx)
		require.NoError(t, err)
		require.Nil(t, conn)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Acquire error", func(t *testing.T) {
		mock := NewPGXPoolMock()

		expectedErr := errors.New("acquire failed")
		mock.ExpectAcquire().WillReturnError(expectedErr)

		conn, err := mock.Acquire(ctx)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, conn)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("AcquireFunc success", func(t *testing.T) {
		mock := NewPGXPoolMock()

		mock.ExpectAcquireFunc()

		called := false
		err := mock.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			called = true
			return nil
		})
		require.NoError(t, err)
		require.True(t, called)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("AcquireFunc error", func(t *testing.T) {
		mock := NewPGXPoolMock()

		expectedErr := errors.New("acquire func failed")
		mock.ExpectAcquireFunc().WillReturnError(expectedErr)

		err := mock.AcquireFunc(ctx, func(conn *pgxpool.Conn) error {
			return nil
		})
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("AcquireAllIdle success", func(t *testing.T) {
		mock := NewPGXPoolMock()

		expectedConns := []*pgxpool.Conn{}
		mock.ExpectAcquireAllIdle().WillReturnConns(expectedConns)

		conns := mock.AcquireAllIdle(ctx)
		require.Equal(t, expectedConns, conns)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Prepare success", func(t *testing.T) {
		mock := NewPGXPoolMock()

		name := "test_stmt"
		sql := "SELECT * FROM users WHERE id = $1"
		mock.ExpectPrepare(name, sql)

		desc, err := mock.Prepare(ctx, name, sql)
		require.NoError(t, err)
		require.NotNil(t, desc)
		require.Equal(t, name, desc.Name)
		require.Equal(t, sql, desc.SQL)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("Prepare error", func(t *testing.T) {
		mock := NewPGXPoolMock()

		name := "test_stmt"
		sql := "SELECT * FROM users WHERE id = $1"
		expectedErr := errors.New("prepare failed")
		mock.ExpectPrepare(name, sql).WillReturnError(expectedErr)

		desc, err := mock.Prepare(ctx, name, sql)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Nil(t, desc)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("CopyFrom success", func(t *testing.T) {
		mock := NewPGXPoolMock()

		tableName := pgx.Identifier{"users"}
		columns := []string{"name", "email"}
		mock.ExpectCopyFrom(tableName).WithColumns(columns).WillReturnResult(3)

		rowsAffected, err := mock.CopyFrom(ctx, tableName, columns, nil)
		require.NoError(t, err)
		require.Equal(t, int64(3), rowsAffected)
		require.NoError(t, mock.AllExpectationsMet())
	})

	t.Run("CopyFrom error", func(t *testing.T) {
		mock := NewPGXPoolMock()

		tableName := pgx.Identifier{"users"}
		columns := []string{"name", "email"}
		expectedErr := errors.New("copy from failed")
		mock.ExpectCopyFrom(tableName).WithColumns(columns).WillReturnError(expectedErr)

		rowsAffected, err := mock.CopyFrom(ctx, tableName, columns, nil)
		require.Error(t, err)
		require.Equal(t, expectedErr, err)
		require.Equal(t, int64(0), rowsAffected)
		require.NoError(t, mock.AllExpectationsMet())
	})
}

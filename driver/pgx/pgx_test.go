package pgx_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/pgx"
	"github.com/Kansuler/octobe/v4/driver/pgx/mock"
	"github.com/stretchr/testify/assert"
)

func TestPGXRunInTransactionCommits(t *testing.T) {
	m := mock.NewPGXConn()
	m.ExpectBeginTx()
	name := "Some name"
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		err = session.ExecuteNoResult(ctx, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		product, err := session.Execute(ctx, AddProduct(name))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		assert.Equal(t, name, product.Name)
		assert.NotZero(t, product.ID)

		products, err := session.Execute(ctx, ProductsByName(name))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		if assert.Equal(t, 1, len(products)) {
			assert.Equal(t, name, products[0].Name)
			assert.NotZero(t, products[0].ID)
		}
		return nil
	})

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXManualTransactionCommits(t *testing.T) {
	m := mock.NewPGXConn()
	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.ExecuteNoResult(ctx, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	product, err := session.Execute(ctx, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product.Name)
	assert.NotZero(t, product.ID)

	products, err := session.Execute(ctx, ProductsByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if assert.Equal(t, 1, len(products)) {
		assert.Equal(t, name, products[0].Name)
		assert.NotZero(t, products[0].ID)
	}

	err = session.Commit(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXNonTransactionalSessionExecutesHandlers(t *testing.T) {
	m := mock.NewPGXConn()
	name := "Some name"

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Session(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.ExecuteNoResult(ctx, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	product, err := session.Execute(ctx, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product.Name)
	assert.NotZero(t, product.ID)

	products, err := session.Execute(ctx, ProductsByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if assert.Equal(t, 1, len(products)) {
		assert.Equal(t, name, products[0].Name)
		assert.NotZero(t, products[0].ID)
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func Migration() octobe.NoResultHandler[pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) error {
		query := newQuery(`
			CREATE TABLE IF NOT EXISTS products (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		_, err := query.Exec(ctx)
		return err
	}
}

type Product struct {
	ID   int
	Name string
}

func AddProduct(name string) octobe.Handler[Product, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Product, error) {
		var product Product
		query := newQuery(`
			INSERT INTO products (name) VALUES ($1) RETURNING id, name;
		`)

		query.WithArgs(name)
		err := query.QueryRow(ctx, &product.ID, &product.Name)
		return product, err
	}
}

func ProductsByName(name string) octobe.Handler[[]Product, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) ([]Product, error) {
		var products []Product
		query := newQuery(`
			SELECT id, name FROM products WHERE name = $1;
		`)

		query.WithArgs(name)
		err := query.Query(ctx, func(rows pgx.Rows) error {
			if rows.Next() {
				var product Product
				err := rows.Scan(&product.ID, &product.Name)
				if err != nil {
					return err
				}
				products = append(products, product)
			}

			return nil
		})
		return products, err
	}
}

func TestPGXRunInTransactionRollsBackOnError(t *testing.T) {
	m := mock.NewPGXConn()
	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	expectedErr := errors.New("something went wrong")
	err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		err = session.ExecuteNoResult(ctx, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		return expectedErr
	}, pgx.WithTxOptions(pgx.TxOptions{}))

	assert.Equal(t, expectedErr, err)

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXRunInTransactionRollsBackOnPanic(t *testing.T) {
	m := mock.NewPGXConn()
	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	panicMsg := "oh no!"
	defer func() {
		p := recover()
		assert.Equal(t, panicMsg, p)

		err = ob.Close(ctx)
		assert.NoError(t, err)
		assert.NoError(t, m.AllExpectationsMet())
	}()

	_ = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		err = session.ExecuteNoResult(ctx, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		panic(panicMsg)
	}, pgx.WithTxOptions(pgx.TxOptions{}))
}

func TestPGXManualTransactionRollsBack(t *testing.T) {
	m := mock.NewPGXConn()
	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.ExecuteNoResult(ctx, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = session.Execute(ctx, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Rollback(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXStatementCannotBeReused(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		m := mock.NewPGXConn()
		m.ExpectExec("CREATE TABLE").WillReturnResult(mock.NewResult("", 0))
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(ctx context.Context, newQuery pgx.QueryFactory) error {
			query := newQuery(`CREATE TABLE`)
			_, err := query.Exec(ctx)
			if err != nil {
				return err
			}
			// Use it again
			_, err = query.Exec(ctx)
			return err
		}

		err = session.ExecuteNoResult(ctx, handler)
		assert.ErrorIs(t, err, octobe.ErrStatementAlreadyExecuted)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("QueryRow", func(t *testing.T) {
		m := mock.NewPGXConn()
		name := "Some name"

		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewRow(1, name))
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(ctx context.Context, newQuery pgx.QueryFactory) error {
			query := newQuery(`SELECT`)
			var p Product
			err := query.QueryRow(ctx, &p.ID, &p.Name)
			if err != nil {
				return err
			}
			// Use it again
			err = query.QueryRow(ctx, &p.ID, &p.Name)
			return err
		}

		err = session.ExecuteNoResult(ctx, handler)
		assert.ErrorIs(t, err, octobe.ErrStatementAlreadyExecuted)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("Query", func(t *testing.T) {
		m := mock.NewPGXConn()
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewRows([]string{"id", "name"}))
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(ctx context.Context, newQuery pgx.QueryFactory) error {
			query := newQuery(`SELECT`)
			err := query.Query(ctx, func(rows pgx.Rows) error {
				return nil
			})
			if err != nil {
				return err
			}
			// Use it again
			err = query.Query(ctx, func(rows pgx.Rows) error {
				return nil
			})
			return err
		}

		err = session.ExecuteNoResult(ctx, handler)
		assert.ErrorIs(t, err, octobe.ErrStatementAlreadyExecuted)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestOpenPGXWithConnRejectsNil(t *testing.T) {
	_, err := octobe.New(pgx.OpenWithConn(nil))
	assert.Error(t, err)
	assert.Equal(t, "conn is nil", err.Error())
}

func TestPGXBeginTxReturnsDriverError(t *testing.T) {
	m := mock.NewPGXConn()
	expectedErr := errors.New("begin error")
	m.ExpectBeginTx().WillReturnError(expectedErr)
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	_, err = ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXCommitReturnsDriverError(t *testing.T) {
	m := mock.NewPGXConn()
	expectedErr := errors.New("commit error")
	m.ExpectBeginTx()
	m.ExpectCommit().WillReturnError(expectedErr)
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit(ctx)
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXStatementExecError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("exec error")
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			query := newQuery("INSERT")
			_, err := query.Exec(ctx)
			return err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("with tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("exec error")
		m.ExpectBeginTx()
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
			err := session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
				query := newQuery("INSERT")
				_, err := query.Exec(ctx)
				return err
			})
			return err
		}, pgx.WithTxOptions(pgx.TxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestPGXStatementQueryRowError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("query row error")
		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewRow().WillReturnError(expectedErr))
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = session.Execute(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) (Product, error) {
			var p Product
			query := newQuery("SELECT")
			err := query.QueryRow(ctx, &p.ID, &p.Name)
			return p, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("with tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("query row error")
		m.ExpectBeginTx()
		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewRow().WillReturnError(expectedErr))
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
			_, err := session.Execute(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) (Product, error) {
				var p Product
				query := newQuery("SELECT")
				err := query.QueryRow(ctx, &p.ID, &p.Name)
				return p, err
			})
			return err
		}, pgx.WithTxOptions(pgx.TxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestPGXStatementQueryError(t *testing.T) {
	t.Run("query error without tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("query error")
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			query := newQuery("SELECT")
			err := query.Query(ctx, func(rows pgx.Rows) error { return nil })
			return err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("query error with tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("query error")
		m.ExpectBeginTx()
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
			err := session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
				query := newQuery("SELECT")
				err := query.Query(ctx, func(rows pgx.Rows) error { return nil })
				return err
			})
			return err
		}, pgx.WithTxOptions(pgx.TxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("callback error without tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("callback error")
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewRows([]string{"id"}).AddRow(1))
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			query := newQuery("SELECT")
			err := query.Query(ctx, func(rows pgx.Rows) error { return expectedErr })
			return err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("callback error with tx", func(t *testing.T) {
		m := mock.NewPGXConn()
		expectedErr := errors.New("callback error")
		m.ExpectBeginTx()
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewRows([]string{"id"}).AddRow(1))
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(pgx.OpenWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
			err := session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
				query := newQuery("SELECT")
				err := query.Query(ctx, func(rows pgx.Rows) error { return expectedErr })
				return err
			})
			return err
		}, pgx.WithTxOptions(pgx.TxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

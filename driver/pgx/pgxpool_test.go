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

func TestPGXPoolRunInTransactionCommits(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		err := session.ExecuteNoResult(ctx, Migration())
		if !assert.NoError(t, err) {
			return err
		}

		p, err := session.Execute(ctx, AddProduct(name))
		if !assert.NoError(t, err) {
			return err
		}

		if !assert.Equal(t, 1, p.ID) {
			return errors.New("expected ID to be 1")
		}

		if !assert.Equal(t, name, p.Name) {
			return errors.New("expected name to be " + name)
		}

		products, err := session.Execute(ctx, ProductsByName(name))
		if !assert.NoError(t, err) {
			return err
		}

		if !assert.Len(t, products, 1) {
			return errors.New("expected 1 product")
		}

		if !assert.Equal(t, 1, products[0].ID) {
			return errors.New("expected ID to be 1")
		}

		if !assert.Equal(t, name, products[0].Name) {
			return errors.New("expected name to be " + name)
		}

		return nil
	})

	assert.NoError(t, err)
	assert.NoError(t, ob.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolManualTransactionCommits(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.ExecuteNoResult(ctx, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	p, err := session.Execute(ctx, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, p.ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, p.Name) {
		t.FailNow()
	}

	products, err := session.Execute(ctx, ProductsByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Len(t, products, 1) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, products[0].ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, products[0].Name) {
		t.FailNow()
	}

	err = session.Commit(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolNonTransactionalSessionExecutesHandlers(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	name := "Some name"

	m.ExpectAcquire()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectRelease()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Session(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.ExecuteNoResult(ctx, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	p, err := session.Execute(ctx, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, p.ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, p.Name) {
		t.FailNow()
	}

	products, err := session.Execute(ctx, ProductsByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Len(t, products, 1) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, products[0].ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, products[0].Name) {
		t.FailNow()
	}

	assert.NoError(t, session.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolNonTransactionalSessionCloseReleasesConnection(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	m.ExpectAcquire()
	m.ExpectRelease()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Session(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.NoError(t, session.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolRunInTransactionRollsBackOnError(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	expectedErr := errors.New("some error")

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnError(expectedErr)
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		err := session.ExecuteNoResult(ctx, Migration())
		return err
	}, pgx.WithTxOptions(pgx.TxOptions{}))

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.NoError(t, ob.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolRunInTransactionRollsBackOnPanic(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Panics(t, func() {
		_ = ob.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
			err := session.ExecuteNoResult(ctx, Migration())
			if err != nil {
				return err
			}
			panic("some panic")
		}, pgx.WithTxOptions(pgx.TxOptions{}))
	})

	assert.NoError(t, ob.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolManualTransactionRollsBack(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectRollback()

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.ExecuteNoResult(ctx, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	p, err := session.Execute(ctx, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, p.ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, p.Name) {
		t.FailNow()
	}

	err = session.Rollback(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolStatementCannotBeReused(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	name := "Some name"

	m.ExpectAcquire()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").Contains().WillReturnResult(mock.NewResult("", 0))

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Session(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}
	defer func() { assert.NoError(t, session.Close(ctx)) }()

	t.Run("Exec", func(t *testing.T) {
		err := session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			statement := newQuery("CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")

			_, err := statement.Exec(ctx)
			assert.NoError(t, err)

			_, err = statement.Exec(ctx)
			return err
		})
		assert.Error(t, err)
		assert.Equal(t, octobe.ErrStatementAlreadyExecuted, err)
	})

	m.ExpectQueryRow("INSERT INTO products").Contains().WithArgs(name).WillReturnRow(mock.NewRow(1, name))

	t.Run("QueryRow", func(t *testing.T) {
		err := session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			statement := newQuery("INSERT INTO products (name) VALUES ($1) RETURNING id, name").WithArgs(name)

			var p Product
			err := statement.QueryRow(ctx, &p.ID, &p.Name)
			assert.NoError(t, err)
			assert.Equal(t, 1, p.ID)
			assert.Equal(t, name, p.Name)

			var p2 Product
			return statement.QueryRow(ctx, &p2.ID, &p2.Name)
		})
		assert.Error(t, err)
		assert.Equal(t, octobe.ErrStatementAlreadyExecuted, err)
	})

	m.ExpectQuery("SELECT id, name FROM products").Contains().WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))

	t.Run("Query", func(t *testing.T) {
		err := session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			statement := newQuery("SELECT id, name FROM products WHERE name = $1").WithArgs(name)

			var products []Product
			err := statement.Query(ctx, func(r pgx.Rows) error {
				for r.Next() {
					var p Product
					if err := r.Scan(&p.ID, &p.Name); err != nil {
						return err
					}
					products = append(products, p)
				}
				return r.Err()
			})
			assert.NoError(t, err)
			assert.Len(t, products, 1)

			return statement.Query(ctx, func(r pgx.Rows) error {
				for r.Next() {
					var p Product
					if err := r.Scan(&p.ID, &p.Name); err != nil {
						return err
					}
				}
				return r.Err()
			})
		})
		assert.Error(t, err)
		assert.Equal(t, octobe.ErrStatementAlreadyExecuted, err)
	})

	assert.NoError(t, m.AllExpectationsMet())
}

func TestOpenPGXWithPoolNil(t *testing.T) {
	_, err := pgx.OpenWithPool(nil)()
	assert.Error(t, err)
}

func TestPGXPoolBeginTxReturnsDriverError(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	expectedErr := errors.New("begin error")
	m.ExpectBeginTx().WillReturnError(expectedErr)

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolCommitReturnsDriverError(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	expectedErr := errors.New("commit error")
	m.ExpectBeginTx()
	m.ExpectCommit().WillReturnError(expectedErr)

	ob, err := octobe.New(pgx.OpenWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit(ctx)
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolStatementExecError(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	expectedErr := errors.New("exec error")

	t.Run("NonTransactional", func(t *testing.T) {
		m.ExpectAcquire()
		m.ExpectExec("INSERT INTO products").Contains().WillReturnError(expectedErr)
		m.ExpectRelease()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		defer func() { assert.NoError(t, session.Close(ctx)) }()

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			_, err := newQuery("INSERT INTO products (name) VALUES ($1)").WithArgs("test").Exec(ctx)
			return err
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("Transactional", func(t *testing.T) {
		m.ExpectBeginTx()
		m.ExpectExec("INSERT INTO products").Contains().WillReturnError(expectedErr)
		m.ExpectRollback()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			_, err := newQuery("INSERT INTO products (name) VALUES ($1)").WithArgs("test").Exec(ctx)
			return err
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.NoError(t, session.Rollback(ctx))
	})

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolStatementQueryRowError(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	expectedErr := errors.New("query row error")

	t.Run("NonTransactional", func(t *testing.T) {
		m.ExpectAcquire()
		row := mock.NewRow().WillReturnError(expectedErr)
		m.ExpectQueryRow("SELECT id FROM products").Contains().WillReturnRow(row)
		m.ExpectRelease()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		defer func() { assert.NoError(t, session.Close(ctx)) }()

		var id int
		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			return newQuery("SELECT id FROM products WHERE name = $1").WithArgs("test").QueryRow(ctx, &id)
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("Transactional", func(t *testing.T) {
		m.ExpectBeginTx()
		row := mock.NewRow().WillReturnError(expectedErr)
		m.ExpectQueryRow("SELECT id FROM products").Contains().WillReturnRow(row)
		m.ExpectRollback()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		var id int
		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			return newQuery("SELECT id FROM products WHERE name = $1").WithArgs("test").QueryRow(ctx, &id)
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.NoError(t, session.Rollback(ctx))
	})

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolStatementQueryError(t *testing.T) {
	m := mock.NewPGXPool()
	ctx := context.Background()

	expectedErr := errors.New("query error")

	t.Run("NonTransactional", func(t *testing.T) {
		m.ExpectAcquire()
		m.ExpectQuery("SELECT id, name FROM products").Contains().WillReturnError(expectedErr)
		m.ExpectRelease()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		defer func() { assert.NoError(t, session.Close(ctx)) }()

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			return newQuery("SELECT id, name FROM products WHERE name = $1").WithArgs("test").Query(ctx, func(r pgx.Rows) error {
				for r.Next() {
					var p Product
					if err := r.Scan(&p.ID, &p.Name); err != nil {
						return err
					}
				}
				return r.Err()
			})
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("Transactional", func(t *testing.T) {
		m.ExpectBeginTx()
		m.ExpectQuery("SELECT id, name FROM products").Contains().WillReturnError(expectedErr)
		m.ExpectRollback()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Transaction(ctx, pgx.WithTxOptions(pgx.TxOptions{}))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			return newQuery("SELECT id, name FROM products WHERE name = $1").WithArgs("test").Query(ctx, func(r pgx.Rows) error {
				for r.Next() {
					var p Product
					if err := r.Scan(&p.ID, &p.Name); err != nil {
						return err
					}
				}
				return r.Err()
			})
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
		assert.NoError(t, session.Rollback(ctx))
	})

	t.Run("CallbackErrorNonTransactional", func(t *testing.T) {
		m.ExpectAcquire()
		rows := mock.NewRows([]string{"id", "name"}).AddRow(1, "test")
		m.ExpectQuery("SELECT id, name FROM products").Contains().WillReturnRows(rows)
		m.ExpectRelease()

		ob, err := octobe.New(pgx.OpenWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Session(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		defer func() { assert.NoError(t, session.Close(ctx)) }()

		err = session.ExecuteNoResult(ctx, func(ctx context.Context, newQuery pgx.QueryFactory) error {
			return newQuery("SELECT id, name FROM products WHERE name = $1").WithArgs("test").Query(ctx, func(r pgx.Rows) error {
				return expectedErr
			})
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	assert.NoError(t, m.AllExpectationsMet())
}

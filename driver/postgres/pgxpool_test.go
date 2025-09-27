package postgres_test

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v3"
	"github.com/Kansuler/octobe/v3/driver/postgres"
	"github.com/Kansuler/octobe/v3/driver/postgres/mock"
	"github.com/stretchr/testify/assert"
)

func TestPGXPoolWithTxInsideStartTransaction(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err := octobe.Execute(session, Migration())
		if !assert.NoError(t, err) {
			return err
		}

		p, err := octobe.Execute(session, AddProduct(name))
		if !assert.NoError(t, err) {
			return err
		}

		if !assert.Equal(t, 1, p.ID) {
			return errors.New("expected ID to be 1")
		}

		if !assert.Equal(t, name, p.Name) {
			return errors.New("expected name to be " + name)
		}

		products, err := octobe.Execute(session, ProductsByName(name))
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
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

	assert.NoError(t, err)
	assert.NoError(t, ob.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTx(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	p, err := octobe.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, p.ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, p.Name) {
		t.FailNow()
	}

	products, err := octobe.Execute(session, ProductsByName(name))
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

	err = session.Commit()
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithoutTx(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	name := "Some name"

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	p, err := octobe.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, p.ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, p.Name) {
		t.FailNow()
	}

	products, err := octobe.Execute(session, ProductsByName(name))
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

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	expectedErr := errors.New("some error")

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnError(expectedErr)
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err := octobe.Execute(session, Migration())
		return err
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.NoError(t, ob.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Panics(t, func() {
		_ = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := octobe.Execute(session, Migration())
			if err != nil {
				return err
			}
			panic("some panic")
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	})

	assert.NoError(t, ob.Close(ctx))
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithTxManualRollback(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectRollback()

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	p, err := octobe.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if !assert.Equal(t, 1, p.ID) {
		t.FailNow()
	}

	if !assert.Equal(t, name, p.Name) {
		t.FailNow()
	}

	err = session.Rollback()
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithoutTxCommit(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit()
	assert.Error(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolWithoutTxRollback(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Rollback()
	assert.Error(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolSegmentUsedTwice(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	name := "Some name"

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	t.Run("Exec", func(t *testing.T) {
		segment := session.Builder()("CREATE TABLE IF NOT EXISTS products (id SERIAL PRIMARY KEY, name TEXT NOT NULL)")

		_, err := segment.Exec()
		assert.NoError(t, err)

		_, err = segment.Exec()
		assert.Error(t, err)
		assert.Equal(t, octobe.ErrAlreadyUsed, err)
	})

	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))

	t.Run("QueryRow", func(t *testing.T) {
		segment := session.Builder()("INSERT INTO products (name) VALUES ($1) RETURNING id, name").Arguments(name)

		var p Product
		err := segment.QueryRow(&p.ID, &p.Name)
		assert.NoError(t, err)
		assert.Equal(t, 1, p.ID)
		assert.Equal(t, name, p.Name)

		var p2 Product
		err = segment.QueryRow(&p2.ID, &p2.Name)
		assert.Error(t, err)
		assert.Equal(t, octobe.ErrAlreadyUsed, err)
	})

	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))

	t.Run("Query", func(t *testing.T) {
		segment := session.Builder()("SELECT id, name FROM products WHERE name = $1").Arguments(name)

		var products []Product
		err := segment.Query(func(r postgres.Rows) error {
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

		var products2 []Product
		err = segment.Query(func(r postgres.Rows) error {
			for r.Next() {
				var p Product
				if err := r.Scan(&p.ID, &p.Name); err != nil {
					return err
				}
				products2 = append(products2, p)
			}
			return r.Err()
		})
		assert.Error(t, err)
		assert.Equal(t, octobe.ErrAlreadyUsed, err)
	})

	assert.NoError(t, m.AllExpectationsMet())
}

func TestOpenPGXWithPoolNil(t *testing.T) {
	_, err := postgres.OpenPGXWithPool(nil)()
	assert.Error(t, err)
}

func TestPGXPoolBeginError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	expectedErr := errors.New("begin error")
	m.ExpectBeginTx().WillReturnError(expectedErr)

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolCommitError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	expectedErr := errors.New("commit error")
	m.ExpectBeginTx()
	m.ExpectCommit().WillReturnError(expectedErr)

	ob, err := octobe.New(postgres.OpenPGXWithPool(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolSegmentExecError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	expectedErr := errors.New("exec error")

	t.Run("WithoutTx", func(t *testing.T) {
		m.ExpectExec("INSERT INTO products").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = session.Builder()("INSERT INTO products (name) VALUES ($1)").Arguments("test").Exec()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("WithTx", func(t *testing.T) {
		m.ExpectBeginTx()
		m.ExpectExec("INSERT INTO products").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = session.Builder()("INSERT INTO products (name) VALUES ($1)").Arguments("test").Exec()
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolSegmentQueryRowError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	expectedErr := errors.New("query row error")

	t.Run("WithoutTx", func(t *testing.T) {
		row := mock.NewRow().WillReturnError(expectedErr)
		m.ExpectQueryRow("SELECT id FROM products").WillReturnRow(row)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		var id int
		err = session.Builder()("SELECT id FROM products WHERE name = $1").Arguments("test").QueryRow(&id)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("WithTx", func(t *testing.T) {
		m.ExpectBeginTx()
		row := mock.NewRow().WillReturnError(expectedErr)
		m.ExpectQueryRow("SELECT id FROM products").WillReturnRow(row)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		var id int
		err = session.Builder()("SELECT id FROM products WHERE name = $1").Arguments("test").QueryRow(&id)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXPoolSegmentQueryError(t *testing.T) {
	m := mock.NewPGXPoolMock()
	ctx := context.Background()

	expectedErr := errors.New("query error")

	t.Run("WithoutTx", func(t *testing.T) {
		m.ExpectQuery("SELECT id, name FROM products").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.Builder()("SELECT id, name FROM products WHERE name = $1").Arguments("test").Query(func(r postgres.Rows) error {
			for r.Next() {
				var p Product
				if err := r.Scan(&p.ID, &p.Name); err != nil {
					return err
				}
			}
			return r.Err()
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("WithTx", func(t *testing.T) {
		m.ExpectBeginTx()
		m.ExpectQuery("SELECT id, name FROM products").WillReturnError(expectedErr)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.Builder()("SELECT id, name FROM products WHERE name = $1").Arguments("test").Query(func(r postgres.Rows) error {
			for r.Next() {
				var p Product
				if err := r.Scan(&p.ID, &p.Name); err != nil {
					return err
				}
			}
			return r.Err()
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	t.Run("CallbackError", func(t *testing.T) {
		rows := mock.NewRows([]string{"id", "name"}).AddRow(1, "test")
		m.ExpectQuery("SELECT id, name FROM products").WillReturnRows(rows)

		ob, err := octobe.New(postgres.OpenPGXWithPool(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		err = session.Builder()("SELECT id, name FROM products WHERE name = $1").Arguments("test").Query(func(r postgres.Rows) error {
			return expectedErr
		})
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
	})

	assert.NoError(t, m.AllExpectationsMet())
}

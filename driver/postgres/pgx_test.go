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

func TestPGXWithTxInsideStartTransaction(t *testing.T) {
	m := mock.NewPGXMock()
	m.ExpectBeginTx()
	name := "Some name"
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err = octobe.Execute(session, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		product, err := octobe.Execute(session, AddProduct(name))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		assert.Equal(t, name, product.Name)
		assert.NotZero(t, product.ID)

		products, err := octobe.Execute(session, ProductsByName(name))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		if assert.Equal(t, 1, len(products)) {
			assert.Equal(t, name, products[0].Name)
			assert.NotZero(t, products[0].ID)
		}
		return nil
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXWithTx(t *testing.T) {
	m := mock.NewPGXMock()
	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectCommit()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	product, err := octobe.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product.Name)
	assert.NotZero(t, product.ID)

	products, err := octobe.Execute(session, ProductsByName(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	if assert.Equal(t, 1, len(products)) {
		assert.Equal(t, name, products[0].Name)
		assert.NotZero(t, products[0].ID)
	}

	err = session.Commit()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXWithoutTx(t *testing.T) {
	m := mock.NewPGXMock()
	name := "Some name"

	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectQuery("SELECT id, name FROM products").WithArgs(name).WillReturnRows(mock.NewRows([]string{"id", "name"}).AddRow(1, name))
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	product, err := octobe.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product.Name)
	assert.NotZero(t, product.ID)

	products, err := octobe.Execute(session, ProductsByName(name))
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

func Migration() octobe.Handler[octobe.Void, postgres.Builder] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`
			CREATE TABLE IF NOT EXISTS products (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL
			);
		`)
		_, err := query.Exec()
		return nil, err
	}
}

type Product struct {
	ID   int
	Name string
}

func AddProduct(name string) octobe.Handler[Product, postgres.Builder] {
	return func(builder postgres.Builder) (Product, error) {
		var product Product
		query := builder(`
			INSERT INTO products (name) VALUES ($1) RETURNING id, name;
		`)

		query.Arguments(name)
		err := query.QueryRow(&product.ID, &product.Name)
		return product, err
	}
}

func ProductsByName(name string) octobe.Handler[[]Product, postgres.Builder] {
	return func(builder postgres.Builder) ([]Product, error) {
		var products []Product
		query := builder(`
			SELECT id, name FROM products WHERE name = $1;
		`)

		query.Arguments(name)
		err := query.Query(func(rows postgres.Rows) error {
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

func TestPGXWithTxInsideStartTransactionRollbackOnError(t *testing.T) {
	m := mock.NewPGXMock()
	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	expectedErr := errors.New("something went wrong")
	err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err = octobe.Execute(session, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		return expectedErr
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

	assert.Equal(t, expectedErr, err)

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXWithTxInsideStartTransactionRollbackOnPanic(t *testing.T) {
	m := mock.NewPGXMock()
	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
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

	_ = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err = octobe.Execute(session, Migration())
		if !assert.NoError(t, err) {
			t.FailNow()
		}
		panic(panicMsg)
	}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
}

func TestPGXWithTxManualRollback(t *testing.T) {
	m := mock.NewPGXMock()
	name := "Some name"

	m.ExpectBeginTx()
	m.ExpectExec("CREATE TABLE IF NOT EXISTS products").WillReturnResult(mock.NewResult("", 0))
	m.ExpectQueryRow("INSERT INTO products").WithArgs(name).WillReturnRow(mock.NewRow(1, name))
	m.ExpectRollback()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, Migration())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	_, err = octobe.Execute(session, AddProduct(name))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Rollback()
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXWithoutTxCommit(t *testing.T) {
	m := mock.NewPGXMock()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit()
	assert.Error(t, err)
	assert.Equal(t, "cannot commit without transaction", err.Error())

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestPGXWithoutTxRollback(t *testing.T) {
	m := mock.NewPGXMock()
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Begin(ctx)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Rollback()
	assert.Error(t, err)
	assert.Equal(t, "cannot rollback without transaction", err.Error())

	err = ob.Close(ctx)
	assert.NoError(t, err)

	assert.NoError(t, m.AllExpectationsMet())
}

func TestSegmentUsedTwice(t *testing.T) {
	t.Run("Exec", func(t *testing.T) {
		m := mock.NewPGXMock()
		m.ExpectExec("CREATE TABLE").WillReturnResult(mock.NewResult("", 0))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(builder postgres.Builder) (octobe.Void, error) {
			query := builder(`CREATE TABLE`)
			_, err := query.Exec()
			if err != nil {
				return nil, err
			}
			// Use it again
			_, err = query.Exec()
			return nil, err
		}

		_, err = octobe.Execute(session, handler)
		assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("QueryRow", func(t *testing.T) {
		m := mock.NewPGXMock()
		name := "Some name"

		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewRow(1, name))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(builder postgres.Builder) (octobe.Void, error) {
			query := builder(`SELECT`)
			var p Product
			err := query.QueryRow(&p.ID, &p.Name)
			if err != nil {
				return nil, err
			}
			// Use it again
			err = query.QueryRow(&p.ID, &p.Name)
			return nil, err
		}

		_, err = octobe.Execute(session, handler)
		assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("Query", func(t *testing.T) {
		m := mock.NewPGXMock()
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewRows([]string{"id", "name"}))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		handler := func(builder postgres.Builder) (octobe.Void, error) {
			query := builder(`SELECT`)
			err := query.Query(func(rows postgres.Rows) error {
				return nil
			})
			if err != nil {
				return nil, err
			}
			// Use it again
			err = query.Query(func(rows postgres.Rows) error {
				return nil
			})
			return nil, err
		}

		_, err = octobe.Execute(session, handler)
		assert.ErrorIs(t, err, octobe.ErrAlreadyUsed)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestOpenWithConnNil(t *testing.T) {
	_, err := octobe.New(postgres.OpenPGXWithConn(nil))
	assert.Error(t, err)
	assert.Equal(t, "conn is nil", err.Error())
}

func TestBeginError(t *testing.T) {
	m := mock.NewPGXMock()
	expectedErr := errors.New("begin error")
	m.ExpectBeginTx().WillReturnError(expectedErr)
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	_, err = ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestCommitError(t *testing.T) {
	m := mock.NewPGXMock()
	expectedErr := errors.New("commit error")
	m.ExpectBeginTx()
	m.ExpectCommit().WillReturnError(expectedErr)
	m.ExpectClose()

	ob, err := octobe.New(postgres.OpenPGXWithConn(m))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	ctx := context.Background()
	session, err := ob.Begin(ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	err = session.Commit()
	assert.ErrorIs(t, err, expectedErr)

	err = ob.Close(ctx)
	assert.NoError(t, err)
	assert.NoError(t, m.AllExpectationsMet())
}

func TestSegmentExecError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("exec error")
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = octobe.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
			query := builder("INSERT")
			_, err := query.Exec()
			return nil, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("with tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("exec error")
		m.ExpectBeginTx()
		m.ExpectExec("INSERT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := octobe.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
				query := builder("INSERT")
				_, err := query.Exec()
				return nil, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestSegmentQueryRowError(t *testing.T) {
	t.Run("without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("query row error")
		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewRow().WillReturnError(expectedErr))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = octobe.Execute(session, func(builder postgres.Builder) (Product, error) {
			var p Product
			query := builder("SELECT")
			err := query.QueryRow(&p.ID, &p.Name)
			return p, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("with tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("query row error")
		m.ExpectBeginTx()
		m.ExpectQueryRow("SELECT").WillReturnRow(mock.NewRow().WillReturnError(expectedErr))
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := octobe.Execute(session, func(builder postgres.Builder) (Product, error) {
				var p Product
				query := builder("SELECT")
				err := query.QueryRow(&p.ID, &p.Name)
				return p, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

func TestSegmentQueryError(t *testing.T) {
	t.Run("query error without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("query error")
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = octobe.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
			query := builder("SELECT")
			err := query.Query(func(rows postgres.Rows) error { return nil })
			return nil, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("query error with tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("query error")
		m.ExpectBeginTx()
		m.ExpectQuery("SELECT").WillReturnError(expectedErr)
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := octobe.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
				query := builder("SELECT")
				err := query.Query(func(rows postgres.Rows) error { return nil })
				return nil, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("callback error without tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("callback error")
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewRows([]string{"id"}).AddRow(1))
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		session, err := ob.Begin(ctx)
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		_, err = octobe.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
			query := builder("SELECT")
			err := query.Query(func(rows postgres.Rows) error { return expectedErr })
			return nil, err
		})
		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})

	t.Run("callback error with tx", func(t *testing.T) {
		m := mock.NewPGXMock()
		expectedErr := errors.New("callback error")
		m.ExpectBeginTx()
		m.ExpectQuery("SELECT").WillReturnRows(mock.NewRows([]string{"id"}).AddRow(1))
		m.ExpectRollback()
		m.ExpectClose()

		ob, err := octobe.New(postgres.OpenPGXWithConn(m))
		if !assert.NoError(t, err) {
			t.FailNow()
		}

		ctx := context.Background()
		err = ob.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
			_, err := octobe.Execute(session, func(builder postgres.Builder) (octobe.Void, error) {
				query := builder("SELECT")
				err := query.Query(func(rows postgres.Rows) error { return expectedErr })
				return nil, err
			})
			return err
		}, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))

		assert.ErrorIs(t, err, expectedErr)

		err = ob.Close(ctx)
		assert.NoError(t, err)

		assert.NoError(t, m.AllExpectationsMet())
	})
}

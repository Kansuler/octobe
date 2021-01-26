package octobe_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Kansuler/octobe"
	"github.com/stretchr/testify/assert"
	"testing"
)

type Product struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

func TestQuery(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectExec("UPDATE products").WithArgs(1).WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectQuery("SELECT id, name FROM products").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow(1, "mirror").AddRow(2, "headset"))
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs(3).WillReturnError(sql.ErrNoRows)

	ctx := context.Background()
	ob := octobe.New(db)
	scheme := ob.Begin(ctx)
	seg := scheme.Segment(`
		UPDATE
			products
		WHERE
			id = $1
	`)
	seg.Arguments(1)
	err = seg.Exec()

	assert.NoError(t, err, "execution should not return error")

	// Segment should not be able to be executed twice
	err = seg.Exec()
	assert.Error(t, err, "executing the same seg twice should error")

	var result []Product

	seg = scheme.Segment(`
		SELECT
			id,
			name
		FROM
			products
	`)
	err = seg.Query(func(rows *sql.Rows) error {
		for rows.Next() {
			var product Product
			err = rows.Scan(
				&product.ID,
				&product.Name,
			)

			if err != nil {
				return err
			}

			result = append(result, product)
		}
		return nil
	})

	assert.NoError(t, err, "query should not return error")

	var product Product
	seg = scheme.Segment(`
		SELECT
			id,
			name
		FROM
			products
		WHERE
			id = $1
	`)
	seg.Arguments(3)
	err = seg.QueryRow(
		&product.ID,
		&product.Name,
	)

	assert.Equal(t, sql.ErrNoRows, err, "expected an sql.ErrNoRows")

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE products").WithArgs(1, "bar").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectCommit()

	ctx := context.Background()
	ob := octobe.New(db)
	tx, err := ob.BeginTx(ctx, nil)
	assert.NoError(t, err, "does not expect begin transaction go get error")
	var id int
	seg := tx.Segment(`
		UPDATE
			products
		SET
			name = $2
		WHERE
			id = $1
		RETURNING id
	`)
	seg.Arguments(1, "bar")
	err = seg.Insert(&id)
	assert.NoError(t, err, "should not return any error")
	assert.Equal(t, 1, id, "id should be 1")

	err = tx.Commit()
	assert.NoError(t, err, "commit shouldn't return any error")

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestTransaction_WatchRollback(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("UPDATE products").WithArgs(1, "bar").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectRollback()

	func() {
		ctx := context.Background()
		ob := octobe.New(db)

		tx, err := ob.BeginTx(ctx, nil)
		assert.NoError(t, err, "does not expect begin transaction go get error")

		defer tx.WatchRollback(func() error {
			return err
		})

		var id int
		seg := tx.Segment(`
			UPDATE
				products
			SET
				name = $2
			WHERE
				id = $1
			RETURNING
				id
		`)
		seg.Arguments(1, "bar")
		err = seg.Insert(&id)
		assert.NoError(t, err, "should not return any error")
		assert.Equal(t, 1, id, "id should be 1")

		err = errors.New("some error occurred, return function before commit happens")
		if err != nil {
			return
		}

		err = tx.Commit()
		assert.NoError(t, err, "commit shouldn't emit error")
	}()

	// we make sure that all expectations were met
	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestTransaction_WithHandlers(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()
	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO products").WithArgs("Testing").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow(1))
	mock.ExpectQuery("SELECT id, name FROM products").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow("1", "test1").AddRow("2", "test2"))
	mock.ExpectCommit()

	handler := func(p *Product) octobe.Handler {
		return func(scheme octobe.Scheme) error {
			seg := scheme.Segment(`
				INSERT INTO
					products(name)
				VALUES($1)
				RETURNING id
			`)

			seg.Arguments(p.Name)

			return seg.Insert(&p.ID)
		}
	}

	ctx := context.Background()
	octo := octobe.New(db)
	scheme, err := octo.BeginTx(ctx, nil)
	assert.NoError(t, err)
	err = scheme.Handle(handler(&Product{Name: "Testing"}))
	assert.NoError(t, err, "handler should not return error")

	handler2 := func(result *[]Product) octobe.Handler {
		return func(scheme octobe.Scheme) error {
			seg := scheme.Segment(`
				SELECT
					id,
					name
				FROM
					products
			`)

			return seg.Query(func(rows *sql.Rows) error {
				for rows.Next() {
					var p Product
					err = rows.Scan(
						&p.ID,
						&p.Name,
					)

					// Will stop function, and return err from seg.Query
					if err != nil {
						return err
					}

					*result = append(*result, p)
				}
				return nil
			})
		}
	}

	var results []Product
	err = scheme.Handle(handler2(&results))
	assert.NoError(t, err)
	for index, result := range results {
		assert.Equal(t, fmt.Sprintf("%d", index+1), result.ID)
		assert.Equal(t, fmt.Sprintf("test%d", index+1), result.Name)
	}

	err = scheme.Commit()
	assert.NoError(t, err)
}

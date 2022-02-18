package example_test

import (
	"database/sql"
	"errors"
	"testing"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Kansuler/octobe"
	"github.com/Kansuler/octobe/example"
	"github.com/stretchr/testify/assert"
)

func TestRun(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery("SELECT id, name FROM products").WithArgs("123").WillReturnRows(sqlmock.NewRows([]string{"id", "name"}).AddRow("123", "test product"))

	ob := octobe.New(db)
	err = example.Run(&ob)
	assert.NoError(t, err)
}

func TestRunFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectQuery("SELECT id, name FROM products").WithArgs("123").WillReturnError(errors.New("an error occurred"))

	ob := octobe.New(db)
	err = example.Run(&ob)
	assert.Error(t, err)
}

func TestRunFailSuppress(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	var returnErr = errors.New("an error occurred")
	mock.ExpectQuery("SELECT id, name FROM products").WithArgs("123").WillReturnError(returnErr)

	ob := octobe.New(db, octobe.SuppressError(returnErr))
	err = example.Run(&ob)
	assert.NoError(t, err)
}

func TestRunTx(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO").WithArgs("Foo product").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("123"))
	mock.ExpectCommit()

	ob := octobe.New(db)
	err = example.RunTx(&ob)
	assert.NoError(t, err)
}

func TestRunTxFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO").WithArgs("Foo product").WillReturnError(sql.ErrTxDone)
	mock.ExpectRollback()

	ob := octobe.New(db)
	err = example.RunTx(&ob)
	assert.Error(t, err)
}

func TestRunTxFailSuppress(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO").WithArgs("Foo product").WillReturnError(sql.ErrTxDone)
	mock.ExpectCommit()

	ob := octobe.New(db, octobe.SuppressError(sql.ErrTxDone))
	err = example.RunTx(&ob)
	assert.NoError(t, err)
}

func TestRunWatchTransaction(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO").WithArgs("test").WillReturnRows(sqlmock.NewRows([]string{"id"}).AddRow("123"))
	mock.ExpectCommit()

	ob := octobe.New(db)
	err = example.RunWatchTransaction(&ob)
	assert.NoError(t, err)
}

func TestRunWatchTransactionFail(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO").WithArgs("test").WillReturnError(sql.ErrConnDone)
	mock.ExpectRollback()

	ob := octobe.New(db)
	err = example.RunWatchTransaction(&ob)
	assert.Error(t, err)
}

func TestRunWatchTransactionFailSuppress(t *testing.T) {
	db, mock, err := sqlmock.New()
	if err != nil {
		t.Fatalf("an error '%s' was not expected when opening a stub database connection", err)
	}

	defer db.Close()

	mock.ExpectBegin()
	mock.ExpectQuery("INSERT INTO").WithArgs("test").WillReturnError(sql.ErrNoRows)
	mock.ExpectCommit()

	ob := octobe.New(db, octobe.SuppressError(sql.ErrNoRows))
	err = example.RunWatchTransaction(&ob)
	assert.NoError(t, err)
}

package example_test

import (
	"github.com/DATA-DOG/go-sqlmock"
	"github.com/Kansuler/octobe"
	"github.com/Kansuler/octobe/example"
	"github.com/stretchr/testify/assert"
	"testing"
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

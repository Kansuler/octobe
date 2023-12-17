package test

import (
	"context"
	"github.com/Kansuler/octobe/v2"
	"github.com/Kansuler/octobe/v2/driver/postgres"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestPostgres(t *testing.T) {
	ctx := context.Background()
	dsn := os.Getenv("DSN")
	if dsn == "" {
		panic("DSN is not set")
	}

	ob, err := octobe.New(postgres.Open(ctx, dsn, postgres.WithTransaction(postgres.TxOptions{})))
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	session, err := ob.Begin(context.Background())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	defer session.WatchRollback(func() error {
		return err
	})

	_, err = Migration(session.Builder())
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	name := uuid.New().String()
	product1, err := AddProduct(session.Builder(), name)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product1.Name)
	assert.NotZero(t, product1.ID)

	product2, err := ProductByName(session.Builder(), name)
	if !assert.NoError(t, err) {
		t.FailNow()
	}

	assert.Equal(t, name, product2.Name)
	assert.NotZero(t, product2.ID)

	err = session.Commit()
	if !assert.NoError(t, err) {
		t.FailNow()
	}
}

func Migration(new postgres.Builder) (bool, error) {
	query := new(`
		CREATE TABLE IF NOT EXISTS products (
			id SERIAL PRIMARY KEY,
		    name TEXT NOT NULL
		);
	`)

	result, err := query.Exec()
	return result.Insert(), err
}

type Product struct {
	ID   int
	Name string
}

func AddProduct(new postgres.Builder, name string) (Product, error) {
	var product Product
	query := new(`
		INSERT INTO products (name) VALUES ($1) RETURNING id, name;
	`)

	query.Arguments(name)
	err := query.QueryRow(&product.ID, &product.Name)
	return product, err
}

func ProductByName(new postgres.Builder, name string) (Product, error) {
	var product Product
	query := new(`
		SELECT id, name FROM products WHERE name = $1;
	`)

	query.Arguments(name)
	err := query.QueryRow(&product.ID, &product.Name)
	return product, err
}

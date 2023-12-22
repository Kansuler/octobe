package query

import (
	"github.com/Kansuler/octobe/v2"
	"github.com/Kansuler/octobe/v2/driver/postgres"
)

func Migration() postgres.Handler[octobe.Void] {
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

func AddProduct(name string) postgres.Handler[Product] {
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

func ProductByName(name string) postgres.Handler[Product] {
	return func(builder postgres.Builder) (Product, error) {
		var product Product
		query := builder(`
			SELECT id, name FROM products WHERE name = $1;
		`)

		query.Arguments(name)
		err := query.QueryRow(&product.ID, &product.Name)
		return product, err
	}
}

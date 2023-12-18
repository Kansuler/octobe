# ![Alt text](https://raw.github.com/Kansuler/octobe/master/doc/octobe_logo.svg)

![License](https://img.shields.io/github/license/Kansuler/octobe) ![Tag](https://img.shields.io/github/v/tag/Kansuler/octobe) ![Version](https://img.shields.io/github/go-mod/go-version/Kansuler/octobe) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/492e6729782b471788994a72f2359f39)](https://www.codacy.com/gh/Kansuler/octobe/dashboard?utm_source=github.com&utm_medium=referral&utm_content=Kansuler/octobe&utm_campaign=Badge_Grade) [![Go Reference](https://pkg.go.dev/badge/github.com/Kansuler/octobe.svg)](https://pkg.go.dev/github.com/Kansuler/octobe)

A slim golang package for programmers that love to write raw SQL, but has a problem with boilerplate code. This package
will help you structure and  the way you work with your database.

The main advantage with this library is to enable developers to build a predictable and consistent database layer
without losing the feeling of freedom. The octobe library draws inspiration from http handlers, but where handlers
interface with the database instead.

Read package documentation at
[https://pkg.go.dev/github.com/Kansuler/octobe](https://pkg.go.dev/github.com/Kansuler/octobe)

## Usage

### Postgres Example

```go
package main

import (
	"context"
	"github.com/Kansuler/octobe/v2"
	"github.com/Kansuler/octobe/v2/driver/postgres"
	"github.com/google/uuid"
	"os"
)

func main() {
    ctx := context.Background()
    dsn := os.Getenv("DSN")
    if dsn == "" {
        panic("DSN is not set")
    }

    // Create a new octobe instance with a postgres driver, insert optional options for configuration that applies to
    // every session.
    o, err := octobe.New(postgres.Open(ctx, dsn, postgres.WithTransaction(postgres.TxOptions{})))
    if err != nil {
        panic(err)
    }

    // Begin a new session, since `postgres.WithTransaction` is set, this will start a postgres transaction.
    session, err := o.Begin(context.Background())
    if err != nil {
        panic(err)
    }

    // WatchRollback will rollback the transaction if var err is not nil when the function returns.
    defer session.WatchRollback(func() error {
        return err
    })

    name := uuid.New().String()

    // Insert a new product into the database, and return a Product struct.
    product1, err := octobe.Execute(session, AddProduct(name))
    if err != nil {
        panic(err)
    }

    // Select the product from the database by name, and return a Product struct.
    product2, err := octobe.Execute(session, ProductByName(name))
    if err != nil {
        panic(err)
    }

    // Commit the transaction, if err is not nil, the transaction will be rolled back via WatchRollback.
    err = session.Commit()
    if err != nil {
        panic(err)
    }
}

// Product is a model that represents a product in the database
type Product struct {
    ID   int
    Name string
}

// AddProduct is an octobe handler that will insert a product into the database, and return a product model.
// In the octobe.Handler signature the first generic is the type of driver builder, and the second is the returned type.
func AddProduct(name string) octobe.Handler[postgres.Builder, Product] {
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


// ProductByName is an octobe handler that will select a product from the database by name, and return a product model.
// In the octobe.Handler signature the first generic is the type of driver builder, and the second is the returned type.
func ProductByName(name string) octobe.Handler[postgres.Builder, Product] {
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
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

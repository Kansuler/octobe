package example

import (
	"context"
	"github.com/Kansuler/octobe"
	"github.com/Kansuler/octobe/example/database"
)

// RunTx - Retrieve a octobe instance, begin a scheme with a
// database transaction.
func RunTx(ob *octobe.Octobe) error {
	ctx := context.Background()
	scheme, err := ob.BeginTx(ctx, nil)
	if err != nil {
		return err
	}

	// If err isn't nil, perform rollback at end of function
	defer scheme.WatchRollback(func() error {
		return err
	})

	product := database.Product{
		Name: "Foo product",
	}

	err = scheme.Handle(database.InsertProduct(&product))
	if err != nil {
		return err
	}

	return scheme.Commit()
}

// Run - Retrieve a octobe instance, begin a scheme without a
// database transaction.
func Run(ob *octobe.Octobe) error {
	ctx := context.Background()
	scheme := ob.Begin(ctx)

	var p database.Product
	err := scheme.Handle(database.ProductByID("123", &p))
	if err != nil {
		return err
	}

	return nil
}

// RunWatchTransaction - Octobe handle most of the transaction logic
// against database.
func RunWatchTransaction(ob *octobe.Octobe) error {
	ctx := context.Background()

	// WatchTransaction will start a transaction against database, if func returns
	// error it will perform a rollback of transaction. If err is nil it will do commit
	err := ob.WatchTransaction(ctx, func(scheme octobe.Scheme) error {
		return scheme.Handle(database.InsertProduct(&database.Product{Name: "test"}))
	})

	if err != nil {
		// log error
	}

	return err
}

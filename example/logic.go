package example

import (
	"context"
	"database/sql"

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

	h := database.Handler{}
	err = scheme.Handle(h.InsertProduct(&product))
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
	h := database.Handler{}
	err := scheme.Handle(h.ProductByID(1, &p))
	if err != nil {
		return err
	}

	return nil
}

// RunSupress - Retrieve a octobe instance, begin a scheme without a
// database transaction, fail it and use the SuppressError option for sql.ErrNoRows
func RunSupress(ob *octobe.Octobe) error {
	ctx := context.Background()
	scheme := ob.Begin(ctx)

	var p database.Product
	h := database.Handler{}
	err := scheme.Handle(h.ProductByID(1, &p), octobe.SuppressError(sql.ErrNoRows))
	if err != nil {
		return err
	}

	return nil
}

// RunWatchTransaction - Octobe handle most of the transaction logic
// against database.
func RunWatchTransaction(ob *octobe.Octobe) error {
	ctx := context.Background()

	h := database.Handler{}
	// WatchTransaction will start a transaction against database, if func returns
	// error it will perform a rollback of transaction. If err is nil it will do commit
	return ob.WatchTransaction(ctx, func(scheme *octobe.Scheme) error {
		return scheme.Handle(h.InsertProduct(&database.Product{Name: "test"}))
	})
}

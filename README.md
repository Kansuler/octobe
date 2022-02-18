# ![Alt text](https://raw.github.com/Kansuler/octobe/master/doc/octobe_logo.svg)

![License](https://img.shields.io/github/license/Kansuler/octobe) ![Tag](https://img.shields.io/github/v/tag/Kansuler/octobe) ![Version](https://img.shields.io/github/go-mod/go-version/Kansuler/octobe) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/492e6729782b471788994a72f2359f39)](https://www.codacy.com/gh/Kansuler/octobe/dashboard?utm_source=github.com&utm_medium=referral&utm_content=Kansuler/octobe&utm_campaign=Badge_Grade) [![Go Reference](https://pkg.go.dev/badge/github.com/Kansuler/octobe.svg)](https://pkg.go.dev/github.com/Kansuler/octobe)

A slim golang package for programmers that love to write raw SQL, but has a problem with boilerplate code. This package will help you structure and unify the way you work with your database.

The main advantage with this library is to enable developers to build a predictable and consistent database layer without losing the feeling of freedom. The octobe library draws inspiration from http handlers, but where handlers interface with the database instead.

Read package documentation at
[https://pkg.go.dev/github.com/Kansuler/octobe](https://pkg.go.dev/github.com/Kansuler/octobe)

## Usage

### Basic Handler

Run a simple query where you chain database handlers.

```go
func Method(db *sql.DB, ctx context.Context) error {
  // New creates an octobe context around an *sql.DB instance
  // SuppressError is option and can be used to ignore specific errors, like sql.ErrNoRows"
  ob := octobe.New(db, octobe.SuppressError(sql.ErrNoRows))

  // Begin is used to start without a database transaction
  scheme := ob.Begin(ctx)

  var p1 Product
  // Handle a database query in another method, perfect for separating out queries to a database package
  err := scheme.Handle(SelectNameHandler(1, &p1))
  if err != nil {
    return err
  }

  var p2 Product
  err := scheme.Handle(SelectNameHandler(2, &p2))
    if err != nil {
    return err
  }

  return
}

// Handler func that implements the octobe.Handler
func SelectNameHandler(id string, p *Product) octobe.Handler {
  return func(scheme *octobe.Scheme) error {
    // A segment is a specific query, you can chain many queries in here, or split chained logic into multiple handler funcs if you'd like.
    seg := scheme.Segment(`
      SELECT name FROM products WHERE id = $1;
    `)

    // Arguments takes any input to the query
    seg.Arguments(id)

    // Segment has all the normal methods you expect such as QueryRow, Query and Exec.
    return seg.QueryRow(&p.Name)
  }
}
```

### Transaction

Run a query with transaction, and with a handler.

```go
func Method(db *sql.DB, ctx context.Context) error {
  ob := octobe.New(db)
  scheme, err := ob.BeginTx(ctx)
  if err != nil {
    return err
  }

  // WatchRollback returns error that is defined in the scope of this Method.
  // if err is not nil, octobe will perform a rollback.
  defer scheme.WatchRollback(func() error {
    return err
  })

  p := Product{Name: "home made baguette"}
  err = scheme.Handle(InsertProduct(&p))
  if err != nil {
    return err
  }

  // Finish with a commit
  return scheme.Commit()
}

// InsertProduct will take a pointer of a product, and insert it
// This method could be in a separate package.
func InsertProduct(p *Product) octobe.Handler {
  return func(scheme *octobe.Scheme) error {
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
```

### WatchTransaction

This is a method that can watch the whole transaction, and where you don't have to define rollback or commit.

WatchTransaction will rollback in case error is returned, otherwise it will proceed to commit.

```go
func Method(db *sql.DB, ctx context.Context) error {
  ob := octobe.New(db)

  // Example of chaining multiple handlers in a transaction
  return ob.WatchTransaction(ctx, func(scheme *octobe.Scheme) error {
    p1 := Product{Name: "home made baguette"}
    err := scheme.Handle(InsertProduct(&p1))
    if err != nil {
      return err
    }

    // Execute other non-database logic that can return an error and rollback the transaction
    err = anotherFunctionWithLogic()
    if err != nil {
      return err
    }

    p2 := Product{Name: "another home made baguette"}
    err = scheme.Handle(InsertProduct(&p2))
    if err != nil {
      return err
    }

    return nil
  })
}

// InsertProduct will take a pointer of a product, and insert it
// This method could be in a separate package.
func InsertProduct(p *Product) octobe.Handler {
  return func(scheme *octobe.Scheme) error {
    seg := scheme.Segment(`
      INSERT INTO
        products(name)
      VALUES($1)
      RETURNING id
    `)

    seg.Arguments(p.Name)

    // Insert is a helper method to do QueryRow and scan of RETURNING from query.
    return seg.Insert(&p.ID)
  }
}
```

## Contributing

Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

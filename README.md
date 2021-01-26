# ![Alt text](https://raw.github.com/Kansuler/octobe/master/doc/octobe_logo.svg)

![License](https://img.shields.io/github/license/Kansuler/octobe) ![Tag](https://img.shields.io/github/v/tag/Kansuler/octobe) ![Version](https://img.shields.io/github/go-mod/go-version/Kansuler/octobe) [![Codacy Badge](https://app.codacy.com/project/badge/Grade/492e6729782b471788994a72f2359f39)](https://www.codacy.com/gh/Kansuler/octobe/dashboard?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=Kansuler/octobe&amp;utm_campaign=Badge_Grade)

A slim golang package for programmers that love to write raw SQL, but has a problem with boilerplate code. This package will help you structure and unify the way you work with your database.

[https://pkg.go.dev/github.com/Kansuler/octobe](https://pkg.go.dev/github.com/Kansuler/octobe)

## Usage
### Basic
Run a simple query
```go
func Method(db *sql.DB, ctx context.Context) (p product, err error) {
  ob := octobe.New(db)
  scheme := ob.Begin(ctx)
  
  seg := scheme.Segment(`
    SELECT name FROM products WHERE id = $1;
  `)
  
  seg.Arguments(1)
  
  err = seg.QueryRow(&p.Name)
  return
}
```

### Transaction
Run a query with transaction, and with a handler.
```go
// InsertProduct will take a pointer of a product, and insert it
// This method could be in a separate package.
func InsertProduct(p *Product) octobe.Handler {
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

func Method(db *sql.DB, ctx context.Context) error {
  ob := octobe.New(db)
  scheme, err := ob.BeginTx(ctx)
  if err != nil {
    return err
  }
  
  defer tx.WatchRollback(func() error {
    return err
  })
  
  p := Product{Name: "home made baguette"}
  err = scheme.Handle(InsertProduct(&p))
  if err != nil {
    return err
  }
  
  return scheme.Commit()
}
```

### WatchTransaction
This is a method that can watch the whole transaction, and where you don't have to define rollback or commit.
```go
// InsertProduct will take a pointer of a product, and insert it
// This method could be in a separate package.
func InsertProduct(p *Product) octobe.Handler {
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

func Method(db *sql.DB, ctx context.Context) error {
  ob := octobe.New(db)
  
  p := Product{Name: "home made baguette"}
  return ob.WatchTransaction(ctx, func(scheme octobe.Scheme) error {
    return scheme.Handle(InsertProduct(&p))
  })
}
```

### 

## Contributing
Pull requests are welcome. For major changes, please open an issue first to discuss what you would like to change.

Please make sure to update tests as appropriate.

# ![Octobe Logotype](https://raw.github.com/Kansuler/octobe/master/doc/octobe_logo.svg)

[![Codacy Badge](https://app.codacy.com/project/badge/Coverage/0d33b2e3bd9d410c949845214cb81e3e)](https://app.codacy.com/gh/Kansuler/octobe/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_coverage)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/0d33b2e3bd9d410c949845214cb81e3e)](https://app.codacy.com/gh/Kansuler/octobe/dashboard?utm_source=gh&utm_medium=referral&utm_content=&utm_campaign=Badge_grade)
[![GoDoc](https://pkg.go.dev/badge/github.com/Kansuler/octobe.svg)](https://pkg.go.dev/github.com/Kansuler/octobe/v3)
![MIT License](https://img.shields.io/github/license/Kansuler/octobe)
![Tag](https://img.shields.io/github/v/tag/Kansuler/octobe)
![Version](https://img.shields.io/github/go-mod/go-version/Kansuler/octobe)

**Raw SQL power. Zero boilerplate. One API for any database.**

Stop writing the same transaction management code over and over. Octobe gives you clean, testable database handlers with automatic transaction lifecycle management.

## The Problem vs. The Solution

**Without Octobe** - Messy, repetitive, error-prone:

```go
func CreateUser(db *sql.DB, name string) (*User, error) {
    tx, err := db.Begin()
    if err != nil {
        return nil, err
    }
    defer func() {
        if err != nil {
            tx.Rollback()
            return
        }
        tx.Commit()
    }()

    var user User
    err = tx.QueryRow("INSERT INTO users (name) VALUES ($1) RETURNING id, name", name).
        Scan(&user.ID, &user.Name)
    return &user, err
}
```

**With Octobe** - Clean, structured, automatic:

```go
func CreateUser(name string) postgres.Handler[User] {
    return func(builder postgres.Builder) (User, error) {
        var user User
        query := builder(`INSERT INTO users (name) VALUES ($1) RETURNING id, name`)
        err := query.Arguments(name).QueryRow(&user.ID, &user.Name)
        return user, err
    }
}

// Usage - transaction management is automatic
user, err := postgres.Execute(session, CreateUser("Alice"))
```

## Why Octobe?

✅ **Zero boilerplate** - No more manual transaction begin/commit/rollback

✅ **Raw SQL freedom** - Write the queries you want, not what an ORM allows

✅ **Built for testing** - Mock any database interaction with ease

✅ **Production ready** - Handle panics, errors, and edge cases automatically

## Quick Start

Install:

```bash
go get github.com/Kansuler/octobe/v3
```

Use:

```go
// 1. Create handlers (your SQL logic)
func GetProduct(id int) postgres.Handler[Product] {
    return func(builder postgres.Builder) (Product, error) {
        var p Product
        query := builder(`SELECT id, name FROM products WHERE id = $1`)
        err := query.Arguments(id).QueryRow(&p.ID, &p.Name)
        return p, err
    }
}

// 2. Execute with automatic transaction management
db, _ := octobe.New(postgres.OpenPGXPool(ctx, dsn))
err := db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
    product, err := postgres.Execute(session, GetProduct(123))
    if err != nil {
        return err // Automatic rollback
    }
    fmt.Printf("Product: %+v\n", product)
    return nil // Automatic commit
})
```

That's it. No manual transaction management, no connection handling, no boilerplate.

## Full Example

Here's a complete example showing the handler pattern in action:

```go
package main

import (
    "context"
    "fmt"
    "os"

    "github.com/Kansuler/octobe"
    "github.com/Kansuler/octobe/driver/postgres"
)

type Product struct {
    ID   int
    Name string
}

// Handlers are pure functions that encapsulate SQL logic
func CreateProduct(name string) postgres.Handler[Product] {
    return func(builder postgres.Builder) (Product, error) {
        var product Product
        query := builder(`INSERT INTO products (name) VALUES ($1) RETURNING id, name`)
        err := query.Arguments(name).QueryRow(&product.ID, &product.Name)
        return product, err
    }
}

func GetProduct(id int) postgres.Handler[Product] {
    return func(builder postgres.Builder) (Product, error) {
        var product Product
        query := builder(`SELECT id, name FROM products WHERE id = $1`)
        err := query.Arguments(id).QueryRow(&product.ID, &product.Name)
        return product, err
    }
}

func main() {
    ctx := context.Background()
    db, err := octobe.New(postgres.OpenPGXPool(ctx, os.Getenv("DSN")))
    if err != nil {
        panic(err)
    }
    defer db.Close(ctx)

    // Everything happens in one transaction - automatic begin/commit/rollback
    err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
        // Create product
        product, err := postgres.Execute(session, CreateProduct("Super Widget"))
        if err != nil {
            return err // Automatic rollback on any error
        }

        // Fetch it back
        fetched, err := postgres.Execute(session, GetProduct(product.ID))
        if err != nil {
            return err
        }

        fmt.Printf("Created and fetched: %+v\n", fetched)
        return nil // Automatic commit
    })

    if err != nil {
        panic(err)
    }
}
```

## Testing Made Simple

Mock any handler without touching your database:

```go
func TestCreateProduct(t *testing.T) {
    ctx := context.Background()

    // 1. Create mock
    mockPool := mock.NewPGXPoolMock()
    db, _ := octobe.New(postgres.OpenPGXPoolWithPool(mockPool))

    // 2. Set expectations
    rows := mock.NewMockRow(1, "Super Widget")
    mockPool.ExpectQueryRow("INSERT INTO products").WithArgs("Super Widget").WillReturnRow(rows)

    // 3. Test your handler
    session, _ := db.Begin(ctx)
    product, err := postgres.Execute(session, CreateProduct("Super Widget"))

    // 4. Assert results
    require.NoError(t, err)
    require.Equal(t, 1, product.ID)
    require.NoError(t, mockPool.AllExpectationsMet())
}
```

## Migration Guide

### From database/sql

**Before (database/sql):**

```go
func GetUser(db *sql.DB, id int) (*User, error) {
    tx, err := db.Begin()
    if err != nil {
        return nil, err
    }
    defer func() {
        if err != nil {
            tx.Rollback()
            return
        }
        tx.Commit()
    }()

    var user User
    err = tx.QueryRow("SELECT id, name FROM users WHERE id = ?", id).
        Scan(&user.ID, &user.Name)
    return &user, err
}
```

**After (Octobe):**

```go
func GetUser(id int) postgres.Handler[User] {
    return func(builder postgres.Builder) (User, error) {
        var user User
        err := builder(`SELECT id, name FROM users WHERE id = $1`).
        	Arguments(id).
         	QueryRow(&user.ID, &user.Name)
        return user, err
    }
}

// Usage
user, err := postgres.Execute(session, GetUser(123))
// Or with automatic transaction management:
var user User
err := db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
    user, err := postgres.Execute(session, GetUser(123))
    return err
})
```

### From GORM

**Before (GORM):**

```go
type User struct {
    ID   uint   `gorm:"primaryKey"`
    Name string
}

func GetUserWithPosts(db *gorm.DB, userID uint) (User, []Post, error) {
    var user User
    var posts []Post

    err := db.First(&user, userID).Error
    if err != nil {
        return user, posts, err
    }

    err = db.Where("user_id = ?", userID).Find(&posts).Error
    return user, posts, err
}
```

**After (Octobe):**

```go
func GetUserWithPosts(userID int) postgres.Handler[UserWithPosts] {
    return func(builder postgres.Builder) (UserWithPosts, error) {
        var result UserWithPosts

        // Get user
        userQuery := builder(`SELECT id, name FROM users WHERE id = $1`)
        err := userQuery.Arguments(userID).QueryRow(&result.User.ID, &result.User.Name)
        if err != nil {
            return result, err
        }

        // Get posts
        postsQuery := builder(`SELECT id, title, content FROM posts WHERE user_id = $1`)
        err = postsQuery.Arguments(userID).Query(func(rows postgres.Rows) error {
            for rows.Next() {
                var post Post
                if err := rows.Scan(&post.ID, &post.Title, &post.Content); err != nil {
                    return err
                }
                result.Posts = append(result.Posts, post)
            }
            return rows.Err()
        })

        return result, err
    }
}
```

### From Squirrel

**Before (Squirrel):**

```go
func UpdateUser(db *sql.DB, id int, name string) error {
    sql, args, err := squirrel.
        Update("users").
        Set("name", name).
        Where(squirrel.Eq{"id": id}).
        PlaceholderFormat(squirrel.Dollar).
        ToSql()
    if err != nil {
        return err
    }

    _, err = db.Exec(sql, args...)
    return err
}
```

**After (Octobe):**

```go
func UpdateUser(id int, name string) postgres.Handler[octobe.Void] {
    return func(builder postgres.Builder) (octobe.Void, error) {
        query := builder(`UPDATE users SET name = $1 WHERE id = $2`)
        _, err := query.Arguments(name, id).Exec()
        return nil, err
    }
}
```

### How does Octobe handle connection pooling?

Octobe uses the underlying driver's connection pooling (like pgxpool). Configure your pool settings when creating the driver:

```go
config, _ := pgxpool.ParseConfig(dsn)
config.MaxConns = 50
pool, _ := pgxpool.NewWithConfig(ctx, config)
db, _ := octobe.New(postgres.OpenPGXPoolWithPool(pool))
```

### Can I use Octobe with existing database code?

Yes! Octobe doesn't require you to rewrite everything. You can:

1. Use Octobe for new features
2. Gradually migrate existing code
3. Use both approaches in the same application
4. Extract the underlying connection for direct use when needed

### What about logging and observability?

Add logging middleware to your handlers:

```go
func WithLogging[T any](name string, handler postgres.Handler[T]) postgres.Handler[T] {
    return func(builder postgres.Builder) (T, error) {
        start := time.Now()
        result, err := handler(builder)

        log.Printf("Handler %s took %v, error: %v", name, time.Since(start), err)
        return result, err
    }
}

// Usage
user, err := postgres.Execute(session, WithLogging("GetUser", GetUser(123)))
```

## Installation & Drivers

```bash
# Core package
go get github.com/Kansuler/octobe/v3

# Database drivers
go get github.com/Kansuler/octobe/v3/driver/postgres
```

### Available Drivers

- **PostgreSQL**: Full-featured driver using pgx/v5
- **SQLite**: _Coming soon_
- **MySQL**: _Coming soon_
- **SQL Server**: _Coming soon_

Want to add a driver? Check our [Driver Development Guide](CONTRIBUTING.md#driver-development).

## Examples

Check out the [examples directory](examples/) for complete, runnable examples:

- **[Simple CRUD](examples/simple/)**: Basic operations to get started
- **[Blog Application](examples/blog/)**: Complex real-world example with relationships

## Contributing

We welcome contributions! Here's how to get started:

### Quick Start for Contributors

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Run tests: `docker compose up --abort-on-container-exit`
4. Commit your changes (`git commit -m 'Add amazing feature'`)
5. Push to the branch (`git push origin feature/amazing-feature`)
6. Open a Pull Request

### Development Areas

- **New Database Drivers**: SQLite, MySQL, SQL Server
- **Testing Utilities**: More helper functions and assertions
- **Documentation**: Examples, guides, and API documentation
- **Performance**: Benchmarks and optimizations
- **Tooling**: Code generation, CLI tools, IDE plugins

### Driver Development

Creating a new database driver? Follow these steps:

1. Implement the core interfaces in `driver/yourdb/`
2. Add comprehensive tests
3. Create mock implementations for testing
4. Add examples and documentation
5. Submit a PR with benchmarks

See the [PostgreSQL driver](driver/postgres/) as a reference implementation.

## License

MIT License - see the [LICENSE](LICENSE) file for details.

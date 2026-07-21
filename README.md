# ![Octobe Logotype](https://raw.github.com/Kansuler/octobe/master/doc/octobe_logo.svg)

[![GoDoc](https://pkg.go.dev/badge/github.com/Kansuler/octobe/v4.svg)](https://pkg.go.dev/github.com/Kansuler/octobe/v4)
![MIT License](https://img.shields.io/github/license/Kansuler/octobe)
![Tag](https://img.shields.io/github/v/tag/Kansuler/octobe)
![Version](https://img.shields.io/github/go-mod/go-version/Kansuler/octobe)

**Octobe is a small Go package for raw SQL handlers with transaction management built in.**
Write the SQL you would write for `pgx`, wrap it in typed handler functions, and run those handlers through a shared session or an automatically committed/rolled-back transaction.

Use Octobe when you want:

- raw SQL, not an ORM model layer
- one transaction API for create/read/update flows
- reusable, typed database operations with http-like handlers
- pgx/pgxpool support

## Quick example

```bash
go get github.com/Kansuler/octobe/v4
```

Octobe v4 requires Go 1.27rc2 or newer because the session API uses generic methods.

```go
package users

import (
	"context"
	"os"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/postgres"
)

type User struct {
	ID    int
	Email string
}

const insertUserSQL = `INSERT INTO users (email) VALUES ($1) RETURNING id, email`

func CreateUser(email string) octobe.Handler[User, postgres.Builder] {
	return func(ctx context.Context, sql postgres.Builder) (User, error) {
		var user User
		err := sql(insertUserSQL).
			Arguments(email).
			QueryRow(ctx, &user.ID, &user.Email)
		return user, err
	}
}

func Signup(ctx context.Context, email string) (User, error) {
	db, err := octobe.New(postgres.OpenPGXPool(ctx, os.Getenv("DATABASE_URL")))
	if err != nil {
		return User{}, err
	}
	defer db.Close(ctx)

	var user User
	err = db.StartTransaction(ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		var err error
		user, err = session.Execute(ctx, CreateUser(email))
		return err
	})
	return user, err
}
```

`StartTransaction` commits when the callback returns `nil`, rolls back when it returns an error, and rolls back before re-panicking on panic. Its callback receives a `ManagedSession`, which exposes `Execute`, `ExecuteVoid`, and `ExecuteMany` but not `Builder` or manual lifecycle methods such as `Commit`, `Rollback`, and `Close`.

## What you write

Handlers keep SQL close to the result type. The generic execution methods are bound to sessions, so `session.Execute(ctx, handler)` infers its result type from the handler:

```go
func UsersByDomain(domain string) octobe.Handler[[]User, postgres.Builder] {
	return func(ctx context.Context, sql postgres.Builder) ([]User, error) {
		query := sql(`
			SELECT id, email
			FROM users
			WHERE email LIKE $1
			ORDER BY id
		`)

		var users []User
		err := query.Arguments("%@" + domain).Query(ctx, func(rows postgres.Rows) error {
			for rows.Next() {
				var user User
				if err := rows.Scan(&user.ID, &user.Email); err != nil {
					return err
				}
				users = append(users, user)
			}
			return rows.Err()
		})

		return users, err
	}
}
```

Compose several operations in the same transaction:

```go
err := db.StartTransaction(ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
	user, err := session.Execute(ctx, CreateUser("alice@example.com"))
	if err != nil {
		return err
	}

	return session.ExecuteVoid(ctx, CreateAuditEvent(user.ID, "signup"))
})
```

Use a regular `Session` when you need manual lifecycle or direct builder control. `Begin` creates a non-transactional session; with pgxpool it pins one connection until `Close`:

```go
session, err := db.Begin(ctx)
if err != nil {
	return err
}
defer session.Close(ctx)

user, err := session.Execute(ctx, GetUser(123))
```

`BeginTx` returns the same `Session` API, but the caller must finish it with `Commit`, `Rollback`, or `Close`.

## Why use Octobe instead of another package?

| If you reach for... | Octobe helps when... |
| --- | --- |
| `database/sql` or plain `pgx` | your functions keep repeating begin/commit/rollback and passing `*sql.Tx` or `pgx.Tx` around |
| an ORM | you want explicit SQL, joins, CTEs, vendor features, and hand-written scans |
| a SQL builder | you already know the SQL and do not need a fluent API to generate it |
| repository interfaces | you want small, reusable functions plus driver-level mocks instead of a custom interface per repository |

## Features

- **Session-bound generic execution**: `Session.Execute` and `ManagedSession.Execute` infer concrete result types from typed handlers.
- **Automatic transactions**: `StartTransaction` handles begin, commit, rollback, cleanup, and panic rollback.
- **Restricted managed sessions**: transaction callbacks cannot commit, roll back, close, or access the builder directly.
- **Manual sessions**: use `Begin` or `BeginTx` when you need explicit lifecycle control.
- **Raw SQL execution**: `Exec`, `QueryRow`, and callback-based `Query` map directly to pgx-style operations.
- **PostgreSQL driver**: supports `pgx.Conn`, `pgxpool.Pool`, DSNs, and existing connections/pools.
- **Testing mocks**: `driver/postgres/mock` lets tests expect queries, rows, transactions, commits, rollbacks, and pool behavior.
- **Single-use query segments**: a query segment can only execute once, preventing accidental reuse.

## What Octobe is not

- **Not an ORM**: no model mapping, lazy loading, migrations, relationship management, or generated queries.
- **Not a SQL builder**: Octobe does not construct SQL for you; you provide the statement.
- **Not a database portability layer today**: the current driver is PostgreSQL via pgx/pgxpool.
- **Not a connection pool replacement**: configure pooling on pgxpool, then pass the pool or DSN to Octobe.

## PostgreSQL setup

Create from a DSN:

```go
db, err := octobe.New(postgres.OpenPGXPool(ctx, os.Getenv("DATABASE_URL")))
```

Or use an existing pool:

```go
config, err := pgxpool.ParseConfig(os.Getenv("DATABASE_URL"))
if err != nil {
	return err
}
config.MaxConns = 20

pool, err := pgxpool.NewWithConfig(ctx, config)
if err != nil {
	return err
}

db, err := octobe.New(postgres.OpenPGXWithPool(pool))
```

Set transaction options when needed:

```go
err := db.StartTransaction(
	ctx,
	func(session *octobe.ManagedSession[postgres.Builder]) error {
		return session.ExecuteVoid(ctx, RebuildReport())
	},
	postgres.WithPGXTxOptions(postgres.PGXTxOptions{IsoLevel: pgx.Serializable}),
)
```

## Testing without a database

```go
func TestCreateUser(t *testing.T) {
	ctx := context.Background()
	pgxMock := mock.NewPGXPoolMock()

	db, err := octobe.New(postgres.OpenPGXWithPool(pgxMock))
	require.NoError(t, err)

	pgxMock.ExpectBeginTx()
	pgxMock.ExpectQueryRow(insertUserSQL).
		WithArgs("alice@example.com").
		WillReturnRow(mock.NewRow(1, "alice@example.com"))
	pgxMock.ExpectCommit()

	var user User
	err = db.StartTransaction(ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		var err error
		user, err = session.Execute(ctx, CreateUser("alice@example.com"))
		return err
	})

	require.NoError(t, err)
	require.Equal(t, User{ID: 1, Email: "alice@example.com"}, user)
	require.NoError(t, pgxMock.AllExpectationsMet())
}
```

## Examples

- [Simple CRUD](examples/simple/) shows table setup, create/read/update/delete, and listing rows.
- [Blog application](examples/blog/) shows a larger schema with users, posts, comments, tags, and multi-step transactions.

Run the full test suite with PostgreSQL:

```bash
docker compose up --abort-on-container-exit
```

## Driver development

A driver implements `octobe.Driver`; it does not implement `octobe.Session` or `octobe.ManagedSession`. Octobe owns those public wrappers.

The driver contracts are:

- **`Open`**: driver constructors return an `octobe.Open` function that creates the configured `Driver`.
- **`Driver`**: owns the database connection or pool. `Begin` and `BeginTx` create a driver-specific `Backend` and return `octobe.NewSession(backend)`. `Close` and `Ping` operate on the owned resource.
- **`Backend`**: represents one active session and supplies `Commit`, `Rollback`, `Close`, and `Builder`. A transactional backend must finalize its transaction and release resources; a non-transactional backend must reject commit/rollback and release resources from `Close`.
- **`Session`**: created by `octobe.NewSession` for callers that own lifecycle. It adds `Execute`, `ExecuteVoid`, and `ExecuteMany` to the backend lifecycle and builder operations.
- **`ManagedSession`**: created by `octobe.StartTransaction` for managed callbacks. It exposes only the execute methods, leaving commit and rollback to Octobe.
- **Builder**: the driver-defined value returned by `Backend.Builder` and passed to `Handler` and `VoidHandler` functions.

A driver's `StartTransaction` method can delegate the lifecycle to the package helper:

```go
func (d *driver) StartTransaction(
	ctx context.Context,
	fn func(*octobe.ManagedSession[Builder]) error,
	opts ...octobe.Option[Config],
) error {
	return octobe.StartTransaction[RawDriver](ctx, d, fn, opts...)
}
```

Use the PostgreSQL driver in [`driver/postgres`](driver/postgres/) as the reference implementation.

## License

MIT License. See [LICENSE](LICENSE).

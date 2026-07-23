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
- reusable, typed database operations with HTTP-style handlers
- pgx/pgxpool support

## Quick example

```bash
go get github.com/Kansuler/octobe/v4
```

Octobe v4 requires Go 1.27 or newer because it uses generic methods.

```go
package main

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

func CreateUser(email string) octobe.Handler[User, postgres.QueryFactory] {
	return func(ctx context.Context, query postgres.QueryFactory) (User, error) {
		var user User
		err := query(`INSERT INTO users (email) VALUES ($1) RETURNING id, email`).
			WithArgs(email).
			QueryRow(ctx, &user.ID, &user.Email)
		return user, err
	}
}

func main() {
	db, err := octobe.New(postgres.OpenPGXPool(ctx, os.Getenv("DATABASE_URL")))
	if err != nil {
		return User{}, err
	}
	defer func() { _ = db.Close(ctx) }()

	// Start a session without a transaction
	session, err := db.Session(ctx)
	if err != nil {
		return User{}, err
	}
	defer func() { _ = session.Close(ctx) }()

  user, err := session.Execute(ctx, CreateUser("email@octobe.invalid"))
  if err != nil {
    return User{}, err
  }

	// Run a transaction
  tx, err := session.Transaction(ctx)
  if err != nil {
    return User{}, err
  }
  defer func() { _ = tx.Rollback(ctx) }()

  user, err = tx.Execute(ctx, CreateUser("email@octobe.invalid"))
  if err != nil {
    return User{}, err
  }

  err = tx.Commit(ctx)
  if err != nil {
    return User{}, err
  }

	// Fully managed transaction, if it returns an error, the transaction is rolled back. Otherwise, it is committed.
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[postgres.QueryFactory]) error {
		var err error
		user, err = session.Execute(ctx, CreateUser(email))
		return err
	})

	return user, err
}
```

`RunInTransaction` commits when the callback returns `nil`, rolls back when it returns an error, and rolls back before re-panicking on panic. Its callback receives a `SessionManaged`, which exposes `Execute`, `ExecuteNoResult`, and `ExecuteSequence`, but no direct query-factory or lifecycle methods.

## What you write

Handlers keep SQL close to the result type. The result type parameter belongs to the execution method, so `session.Execute(ctx, handler)` infers `R` from `Handler[R, QF]`:

```go
func UsersByDomain(domain string) octobe.Handler[[]User, postgres.QueryFactory] {
	return func(ctx context.Context, query postgres.QueryFactory) ([]User, error) {
		var users []User
		err := query(`
			SELECT id, email
			FROM users
			WHERE email LIKE $1
			ORDER BY id
		`).
		WithArgs("%@" + domain).
		Query(ctx, func(rows postgres.Rows) error {
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

`ExecuteSequence` uses the same inference and runs handlers that return the same result type in order, and returns a slice of results.

```go
created, err := session.ExecuteSequence(ctx,
	CreateUser("alice@example.com"),
	CreateUser("bob@example.com"),
)
```

`ExecuteSequence` does not start a transaction. Run it on a `SessionManaged` or `SessionTransaction` when the sequence must share a transaction.

Compose several operations in the same transaction:

```go
err := db.RunInTransaction(ctx, func(session *octobe.SessionManaged[postgres.QueryFactory]) error {
	user, err := session.Execute(ctx, CreateUser("alice@example.com"))
	if err != nil {
		return err
	}

	return session.ExecuteNoResult(ctx, CreateAuditEvent(user.ID, "signup"))
})
```

Use `Session` for a non-transactional session with explicit lifecycle control. With pgxpool it pins one connection until `Close`:

```go
session, err := db.Session(ctx)
if err != nil {
	return err
}
defer func() { _ = session.Close(ctx) }()

user, err := session.Execute(ctx, GetUserByID(123))
```

Use `Transaction` for manual transaction control. It returns a `SessionTransaction`, which exposes the three execution methods plus `Commit` and `Rollback`:

```go
tx, err := db.Transaction(ctx)
if err != nil {
	return err
}
defer func() { _ = tx.Rollback(ctx) }()

if err := tx.ExecuteNoResult(ctx, CreateAuditEvent(123, "manual")); err != nil {
	return err
}
return tx.Commit(ctx)
```

## Why use Octobe instead of another package?

| If you reach for... | Octobe helps when... |
| --- | --- |
| `database/sql` or plain `pgx` | your functions keep repeating begin/commit/rollback and passing `*sql.Tx` or `pgx.Tx` around |
| an ORM | you want explicit SQL, joins, CTEs, vendor features, and hand-written scans |
| a SQL builder | you already know the SQL and do not need a fluent API to generate it |
| repository interfaces | you want small, reusable functions plus driver-level mocks instead of a custom interface per repository |

## Features

- **Session-bound generic execution**: `Session.Execute`, `SessionManaged.Execute`, and `SessionTransaction.Execute` infer concrete result types from typed handlers; each session type also exposes `ExecuteNoResult` and `ExecuteSequence`.
- **Automatic transactions**: `RunInTransaction` handles begin, commit, and rollback, including rollback before re-panicking.
- **Restricted managed sessions**: transaction callbacks cannot commit, roll back, close, or directly access the query factory.
- **Explicit lifecycle APIs**: use `Session` for an explicitly closed non-transactional session, or `Transaction` for manual commit/rollback.
- **Raw SQL execution**: `Exec`, `QueryRow`, and callback-based `Query` map directly to pgx-style operations.
- **PostgreSQL driver**: supports `pgx.Conn`, `pgxpool.Pool`, DSNs, and existing connections/pools.
- **Testing mocks**: `driver/postgres/mock` lets tests expect queries, rows, transactions, commits, rollbacks, and pool behavior.
- **Single-use statements**: a statement can only execute once, preventing accidental reuse.

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
err := db.RunInTransaction(
	ctx,
	func(session *octobe.SessionManaged[postgres.QueryFactory]) error {
		return session.ExecuteNoResult(ctx, RebuildReport())
	},
	postgres.WithPGXTxOptions(postgres.PGXTxOptions{IsoLevel: pgx.Serializable}),
)
```

## Testing without a database

```go
func TestCreateUser(t *testing.T) {
	ctx := context.Background()
	pgxMock := mock.NewPGXPool()

	db, err := octobe.New(postgres.OpenPGXWithPool(pgxMock))
	require.NoError(t, err)

	pgxMock.ExpectBeginTx()
	pgxMock.ExpectQueryRow(insertUserSQL).
		WithArgs("alice@example.com").
		WillReturnRow(mock.NewRow(1, "alice@example.com"))
	pgxMock.ExpectCommit()

	var user User
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[postgres.QueryFactory]) error {
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

A driver implements `octobe.Driver`;

The driver contracts are:

- **`OpenFunc`**: driver constructors return an `octobe.OpenFunc` that creates the configured `Driver`.
- **`Driver`**: owns the database connection or pool. `Session` creates a non-transactional backend and wraps it with `octobe.NewSession`; `Transaction` creates a transactional backend and wraps it with `octobe.NewTransaction`. `Close` and `Ping` operate on the owned resource.
- **`Backend`**: represents one active driver session and supplies `Commit`, `Rollback`, `Close`, and `QueryFactory` to Octobe's wrappers.
- **`Session`**: exposes `Close`, `Execute`, `ExecuteNoResult`, and `ExecuteSequence`. Handlers receive the backend query factory internally.
- **`SessionTransaction`**: exposes `Commit`, `Rollback`, and the three execution methods.
- **`SessionManaged`**: created by `octobe.NewSessionManaged` inside `octobe.RunInTransaction`. It exposes only the three execution methods, leaving transaction lifecycle to Octobe.
- **`QueryFactory`**: the driver-defined value returned by `Backend.QueryFactory` and passed to `Handler` and `NoResultHandler` functions.

A driver's `RunInTransaction` method can delegate lifecycle to the package helper. Pass the driver's underlying resource type as the first type argument, as the PostgreSQL pool driver does with `PGXPool`:

```go
func (d *pgxpoolConn) RunInTransaction(
	ctx context.Context,
	fn func(*octobe.SessionManaged[QueryFactory]) error,
	opts ...Option,
) error {
	return octobe.RunInTransaction[PGXPool](ctx, d, fn, opts...)
}
```

Use the PostgreSQL driver in [`driver/postgres`](driver/postgres/) as the reference implementation.

## License

MIT License. See [LICENSE](LICENSE).

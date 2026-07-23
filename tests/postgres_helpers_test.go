package tests

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/postgres"
)

type integrationProduct struct {
	ID   int
	Name string
}

func integrationDSN(t *testing.T) string {
	t.Helper()

	dsn := os.Getenv("DSN")
	if dsn == "" {
		t.Skip("DSN is not set; run these integration tests with docker-compose.yaml")
	}

	return dsn
}

func openPGXWithRetry(t *testing.T, ctx context.Context, dsn string) postgres.PGXDriver {
	t.Helper()

	deadline := time.Now().Add(8 * time.Second)
	var lastErr error

	for {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		db, err := octobe.New(postgres.OpenPGX(attemptCtx, dsn))
		cancel()
		if err == nil {
			pingCtx, pingCancel := context.WithTimeout(ctx, time.Second)
			err = db.Ping(pingCtx)
			pingCancel()
			if err == nil {
				return db
			}
			_ = db.Close(ctx)
		}

		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("connect pgx integration database: %v", lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func openPGXPoolWithRetry(t *testing.T, ctx context.Context, dsn string) postgres.PGXPoolDriver {
	t.Helper()

	deadline := time.Now().Add(8 * time.Second)
	var lastErr error

	for {
		attemptCtx, cancel := context.WithTimeout(ctx, time.Second)
		db, err := octobe.New(postgres.OpenPGXPool(attemptCtx, dsn))
		cancel()
		if err == nil {
			pingCtx, pingCancel := context.WithTimeout(ctx, time.Second)
			err = db.Ping(pingCtx)
			pingCancel()
			if err == nil {
				return db
			}
			_ = db.Close(ctx)
		}

		lastErr = err
		if time.Now().After(deadline) {
			t.Fatalf("connect pgxpool integration database: %v", lastErr)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func migrateProducts(table string) octobe.NoResultHandler[postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) error {
		_, err := newQuery(fmt.Sprintf(`
			CREATE TABLE IF NOT EXISTS %s (
				id SERIAL PRIMARY KEY,
				name TEXT NOT NULL
			);
		`, quoteIdentifier(table))).Exec(ctx)
		return err
	}
}

func truncateProducts(table string) octobe.NoResultHandler[postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) error {
		_, err := newQuery(fmt.Sprintf(`TRUNCATE TABLE %s RESTART IDENTITY`, quoteIdentifier(table))).Exec(ctx)
		return err
	}
}

func dropProducts(table string) octobe.NoResultHandler[postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) error {
		_, err := newQuery(fmt.Sprintf(`DROP TABLE IF EXISTS %s`, quoteIdentifier(table))).Exec(ctx)
		return err
	}
}

func createProduct(table, name string) octobe.Handler[integrationProduct, postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) (integrationProduct, error) {
		var product integrationProduct
		err := newQuery(fmt.Sprintf(`
			INSERT INTO %s (name)
			VALUES ($1)
			RETURNING id, name
		`, quoteIdentifier(table))).WithArgs(name).QueryRow(ctx, &product.ID, &product.Name)
		return product, err
	}
}

func productByID(table string, id int) octobe.Handler[integrationProduct, postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) (integrationProduct, error) {
		var product integrationProduct
		err := newQuery(fmt.Sprintf(`
			SELECT id, name
			FROM %s
			WHERE id = $1
		`, quoteIdentifier(table))).WithArgs(id).QueryRow(ctx, &product.ID, &product.Name)
		return product, err
	}
}

func productsByName(table, name string) octobe.Handler[[]integrationProduct, postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) ([]integrationProduct, error) {
		var products []integrationProduct
		err := newQuery(fmt.Sprintf(`
			SELECT id, name
			FROM %s
			WHERE name = $1
			ORDER BY id
		`, quoteIdentifier(table))).WithArgs(name).Query(ctx, func(rows postgres.Rows) error {
			for rows.Next() {
				var product integrationProduct
				if err := rows.Scan(&product.ID, &product.Name); err != nil {
					return err
				}
				products = append(products, product)
			}
			return rows.Err()
		})
		return products, err
	}
}

func backendPID() octobe.Handler[int, postgres.QueryFactory] {
	return func(ctx context.Context, newQuery postgres.QueryFactory) (int, error) {
		var pid int
		err := newQuery(`SELECT pg_backend_pid()`).QueryRow(ctx, &pid)
		return pid, err
	}
}

func quoteIdentifier(identifier string) string {
	return `"` + strings.ReplaceAll(identifier, `"`, `""`) + `"`
}

// Package main demonstrates basic Octobe usage with simple CRUD operations.
// This example shows the fundamental patterns for database operations using Octobe.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/pgx"
)

// Simple data model
type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Create table handler
func EnsureUsersTable() octobe.NoResultHandler[pgx.QueryFactory] {
	return func(ctx context.Context, query pgx.QueryFactory) error {
		_, err := query(`
			CREATE TABLE IF NOT EXISTS users (
				id SERIAL PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(100) UNIQUE NOT NULL
			)`).Exec(ctx)
		return err
	}
}

// Create user handler
func CreateUser(name, email string) octobe.Handler[User, pgx.QueryFactory] {
	return func(ctx context.Context, query pgx.QueryFactory) (User, error) {
		var user User
		err := query(`
			INSERT INTO users (name, email)
			VALUES ($1, $2)
			RETURNING id, name, email`).
			WithArgs(name, email).
			QueryRow(ctx, &user.ID, &user.Name, &user.Email)
		return user, err
	}
}

// Get user by ID handler
func GetUserByID(id int) octobe.Handler[User, pgx.QueryFactory] {
	return func(ctx context.Context, query pgx.QueryFactory) (User, error) {
		var user User
		err := query(`
			SELECT id, name, email
			FROM users
			WHERE id = $1`).
			WithArgs(id).
			QueryRow(ctx, &user.ID, &user.Name, &user.Email)
		return user, err
	}
}

// Update user handler
func UpdateUser(id int, name, email string) octobe.Handler[User, pgx.QueryFactory] {
	return func(ctx context.Context, query pgx.QueryFactory) (User, error) {
		var user User
		err := query(`
			UPDATE users
			SET name = $1, email = $2
			WHERE id = $3
			RETURNING id, name, email`).
			WithArgs(name, email, id).
			QueryRow(ctx, &user.ID, &user.Name, &user.Email)
		return user, err
	}
}

func main() {
	// Get database URL from environment or use default
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgresql://user:password@localhost:5432/testdb?sslmode=disable"
		log.Printf("Using default database URL. Set DATABASE_URL environment variable to use different database.")
	}

	ctx := context.Background()

	// Step 1: Initialize database connection
	db, err := octobe.New(pgx.OpenPGXPool(ctx, dsn))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			log.Fatalf("Failed to close database: %v", err)
		}
	}()

	// Step 2: Test connection
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	fmt.Println("✓ Connected to database")

	var users []User
	// Create users in a managed transaction, the transaction is rolled back if any error occurs and
	// committed otherwise.
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		// Step 3: Create table (in a transaction)
		err := session.ExecuteNoResult(ctx, EnsureUsersTable())
		if err != nil {
			return err
		}

		// Step 4: Create a user
		users, err = session.ExecuteSequence(ctx,
			CreateUser("Alice Smith", "alice@example.com"),
			CreateUser("Bob Jones", "bob@example.com"),
		)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("✓ Created users table")

	// Open a non-transactional session to retrieve the user
	session, err := db.Session(ctx)
	if err != nil {
		log.Fatalf("Failed to open session: %v", err)
	}
	defer session.Close(ctx) // release acquired conn from pgx pool

	// Retrieve the user
	alice, err := session.Execute(ctx, GetUserByID(users[0].ID))
	if err != nil {
		log.Fatalf("Failed to get user: %v", err)
	}
	fmt.Printf("✓ Retrieved user: %s <%s>\n", alice.Name, alice.Email)

	// Open a transaction to update the user
	tx, err := db.Transaction(ctx)
	if err != nil {
		log.Fatalf("Failed to start transaction: %v", err)
	}
	defer tx.Rollback(ctx) // rollback if commit fails, rollback is no-op if commit succeeds

	alice, err = tx.Execute(ctx, UpdateUser(alice.ID, "Alice Johnson", "alice.johnson@example.com"))
	if err != nil {
		log.Fatalf("Failed to update user: %v", err)
	}
	fmt.Printf("✓ Updated user: %s <%s>\n", alice.Name, alice.Email)

	if err := tx.Commit(ctx); err != nil {
		log.Fatalf("Failed to commit transaction: %v", err)
	}

	fmt.Println("\n🎉 Simple example completed successfully!")
	fmt.Println("\nKey concepts demonstrated:")
	fmt.Println("• Handler pattern for encapsulating SQL operations")
	fmt.Println("• Automatic transaction management with RunInTransaction")
	fmt.Println("• Type-safe query results")
	fmt.Println("• CRUD operations (Create, Read, Update, Delete)")
	fmt.Println("• Error handling with automatic rollback")
}

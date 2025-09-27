// Package main demonstrates basic Octobe usage with simple CRUD operations.
// This example shows the fundamental patterns for database operations using Octobe.
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/Kansuler/octobe/v3"
	"github.com/Kansuler/octobe/v3/driver/postgres"
)

// Simple data model
type User struct {
	ID    int    `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
}

// Create table handler
func CreateUsersTable() octobe.Handler[octobe.Void, postgres.Builder] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`
			CREATE TABLE IF NOT EXISTS users (
				id SERIAL PRIMARY KEY,
				name VARCHAR(100) NOT NULL,
				email VARCHAR(100) UNIQUE NOT NULL
			)`)
		_, err := query.Exec()
		return nil, err
	}
}

// Create user handler
func CreateUser(name, email string) octobe.Handler[User, postgres.Builder] {
	return func(builder postgres.Builder) (User, error) {
		var user User
		query := builder(`
			INSERT INTO users (name, email)
			VALUES ($1, $2)
			RETURNING id, name, email`)
		err := query.Arguments(name, email).QueryRow(&user.ID, &user.Name, &user.Email)
		return user, err
	}
}

// Get user by ID handler
func GetUser(id int) octobe.Handler[User, postgres.Builder] {
	return func(builder postgres.Builder) (User, error) {
		var user User
		query := builder(`
			SELECT id, name, email
			FROM users
			WHERE id = $1`)
		err := query.Arguments(id).QueryRow(&user.ID, &user.Name, &user.Email)
		return user, err
	}
}

// Update user handler
func UpdateUser(id int, name, email string) octobe.Handler[User, postgres.Builder] {
	return func(builder postgres.Builder) (User, error) {
		var user User
		query := builder(`
			UPDATE users
			SET name = $1, email = $2
			WHERE id = $3
			RETURNING id, name, email`)
		err := query.Arguments(name, email, id).QueryRow(&user.ID, &user.Name, &user.Email)
		return user, err
	}
}

// Delete user handler
func DeleteUser(id int) octobe.Handler[octobe.Void, postgres.Builder] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`DELETE FROM users WHERE id = $1`)
		_, err := query.Arguments(id).Exec()
		return nil, err
	}
}

// List all users handler
func ListUsers() octobe.Handler[[]User, postgres.Builder] {
	return func(builder postgres.Builder) ([]User, error) {
		query := builder(`SELECT id, name, email FROM users ORDER BY id`)

		var users []User
		err := query.Query(func(rows postgres.Rows) error {
			for rows.Next() {
				var user User
				if err := rows.Scan(&user.ID, &user.Name, &user.Email); err != nil {
					return err
				}
				users = append(users, user)
			}
			return rows.Err()
		})

		return users, err
	}
}

func main() {
	// Get database URL from environment or use default
	dsn := os.Getenv("DSN")
	if dsn == "" {
		dsn = "postgresql://user:password@localhost:5432/testdb?sslmode=disable"
		log.Printf("Using default DSN. Set DATABASE_URL environment variable to use different database.")
	}

	ctx := context.Background()

	// Step 1: Initialize database connection
	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close(ctx)

	// Step 2: Test connection
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	fmt.Println("✓ Connected to database")

	// Step 3: Create table (in a transaction)
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, CreateUsersTable())
	})
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
	fmt.Println("✓ Created users table")

	// Step 4: Create a user
	var alice User
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		alice, err = octobe.Execute(session, CreateUser("Alice Smith", "alice@example.com"))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to create user: %v", err)
	}
	fmt.Printf("✓ Created user: %s (ID: %d)\n", alice.Name, alice.ID)

	// Step 5: Create another user
	var bob User
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		bob, err = octobe.Execute(session, CreateUser("Bob Jones", "bob@example.com"))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to create user: %v", err)
	}
	fmt.Printf("✓ Created user: %s (ID: %d)\n", bob.Name, bob.ID)

	// Step 6: Read user back
	var retrievedUser User
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		retrievedUser, err = octobe.Execute(session, GetUser(alice.ID))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to get user: %v", err)
	}
	fmt.Printf("✓ Retrieved user: %s <%s>\n", retrievedUser.Name, retrievedUser.Email)

	// Step 7: Update user
	var updatedUser User
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		updatedUser, err = octobe.Execute(session, UpdateUser(alice.ID, "Alice Johnson", "alice.johnson@example.com"))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to update user: %v", err)
	}
	fmt.Printf("✓ Updated user: %s <%s>\n", updatedUser.Name, updatedUser.Email)

	// Step 8: List all users
	var users []User
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		users, err = octobe.Execute(session, ListUsers())
		return err
	})
	if err != nil {
		log.Fatalf("Failed to list users: %v", err)
	}
	fmt.Printf("✓ Found %d users:\n", len(users))
	for _, user := range users {
		fmt.Printf("  - %s <%s> (ID: %d)\n", user.Name, user.Email, user.ID)
	}

	// Step 9: Delete a user
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, DeleteUser(bob.ID))
	})
	if err != nil {
		log.Fatalf("Failed to delete user: %v", err)
	}
	fmt.Printf("✓ Deleted user with ID: %d\n", bob.ID)

	// Step 10: Verify deletion
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		users, err = octobe.Execute(session, ListUsers())
		return err
	})
	if err != nil {
		log.Fatalf("Failed to list users after deletion: %v", err)
	}
	fmt.Printf("✓ Users remaining: %d\n", len(users))

	fmt.Println("\n🎉 Simple example completed successfully!")
	fmt.Println("\nKey concepts demonstrated:")
	fmt.Println("• Handler pattern for encapsulating SQL operations")
	fmt.Println("• Automatic transaction management with StartTransaction")
	fmt.Println("• Type-safe query results")
	fmt.Println("• CRUD operations (Create, Read, Update, Delete)")
	fmt.Println("• Error handling with automatic rollback")
}

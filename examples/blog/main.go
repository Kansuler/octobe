// Package main demonstrates a comprehensive blog application using Octobe
// for database operations. This example shows real-world usage patterns
// including CRUD operations, transactions, and complex queries.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/pgx"
)

// Domain models
type User struct {
	ID        int       `json:"id"`
	Username  string    `json:"username"`
	Email     string    `json:"email"`
	CreatedAt time.Time `json:"created_at"`
}

type Post struct {
	ID        int       `json:"id"`
	Title     string    `json:"title"`
	Content   string    `json:"content"`
	AuthorID  int       `json:"author_id"`
	Author    *User     `json:"author,omitempty"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type Comment struct {
	ID        int       `json:"id"`
	PostID    int       `json:"post_id"`
	AuthorID  int       `json:"author_id"`
	Content   string    `json:"content"`
	CreatedAt time.Time `json:"created_at"`
}

type Tag struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

// Database schema creation
func EnsureSchema() octobe.NoResultHandler[pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) error {
		schema := `
		CREATE TABLE IF NOT EXISTS users (
			id SERIAL PRIMARY KEY,
			username VARCHAR(50) UNIQUE NOT NULL,
			email VARCHAR(100) UNIQUE NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS posts (
			id SERIAL PRIMARY KEY,
			title VARCHAR(200) NOT NULL,
			content TEXT NOT NULL,
			author_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
			created_at TIMESTAMP DEFAULT NOW(),
			updated_at TIMESTAMP DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS comments (
			id SERIAL PRIMARY KEY,
			post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
			author_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
			content TEXT NOT NULL,
			created_at TIMESTAMP DEFAULT NOW()
		);

		CREATE TABLE IF NOT EXISTS tags (
			id SERIAL PRIMARY KEY,
			name VARCHAR(50) UNIQUE NOT NULL
		);

		CREATE TABLE IF NOT EXISTS post_tags (
			post_id INTEGER REFERENCES posts(id) ON DELETE CASCADE,
			tag_id INTEGER REFERENCES tags(id) ON DELETE CASCADE,
			PRIMARY KEY (post_id, tag_id)
		);`

		query := newQuery(schema)
		_, err := query.Exec(ctx)
		return err
	}
}

// User operations
func CreateUser(username, email string) octobe.Handler[User, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (User, error) {
		var user User
		query := newQuery(`
			INSERT INTO users (username, email)
			VALUES ($1, $2)
			RETURNING id, username, email, created_at`)

		err := query.WithArgs(username, email).QueryRow(ctx,
			&user.ID, &user.Username, &user.Email, &user.CreatedAt)
		return user, err
	}
}

func GetUserByID(id int) octobe.Handler[User, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (User, error) {
		var user User
		query := newQuery(`
			SELECT id, username, email, created_at
			FROM users
			WHERE id = $1`)

		err := query.WithArgs(id).QueryRow(ctx,
			&user.ID, &user.Username, &user.Email, &user.CreatedAt)
		return user, err
	}
}

func GetUserByUsername(username string) octobe.Handler[User, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (User, error) {
		var user User
		query := newQuery(`
			SELECT id, username, email, created_at
			FROM users
			WHERE username = $1`)

		err := query.WithArgs(username).QueryRow(ctx,
			&user.ID, &user.Username, &user.Email, &user.CreatedAt)
		return user, err
	}
}

// Post operations
func CreatePost(title, content string, authorID int) octobe.Handler[Post, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Post, error) {
		var post Post
		query := newQuery(`
			INSERT INTO posts (title, content, author_id)
			VALUES ($1, $2, $3)
			RETURNING id, title, content, author_id, created_at, updated_at`)

		err := query.WithArgs(title, content, authorID).QueryRow(ctx,
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt)
		return post, err
	}
}

func GetPostWithAuthor(postID int) octobe.Handler[Post, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Post, error) {
		var post Post
		var author User

		query := newQuery(`
			SELECT
				p.id, p.title, p.content, p.author_id, p.created_at, p.updated_at,
				u.id, u.username, u.email, u.created_at
			FROM posts p
			JOIN users u ON p.author_id = u.id
			WHERE p.id = $1`)

		err := query.WithArgs(postID).QueryRow(ctx,
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt,
			&author.ID, &author.Username, &author.Email, &author.CreatedAt)

		if err == nil {
			post.Author = &author
		}
		return post, err
	}
}

func GetPostsByAuthor(authorID int) octobe.Handler[[]Post, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) ([]Post, error) {
		query := newQuery(`
			SELECT id, title, content, author_id, created_at, updated_at
			FROM posts
			WHERE author_id = $1
			ORDER BY created_at DESC`)

		var posts []Post
		err := query.WithArgs(authorID).Query(ctx, func(rows pgx.Rows) error {
			for rows.Next() {
				var post Post
				if err := rows.Scan(&post.ID, &post.Title, &post.Content,
					&post.AuthorID, &post.CreatedAt, &post.UpdatedAt); err != nil {
					return err
				}
				posts = append(posts, post)
			}
			return rows.Err()
		})

		return posts, err
	}
}

func UpdatePost(postID int, title, content string) octobe.Handler[Post, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Post, error) {
		var post Post
		query := newQuery(`
			UPDATE posts
			SET title = $1, content = $2, updated_at = NOW()
			WHERE id = $3
			RETURNING id, title, content, author_id, created_at, updated_at`)

		err := query.WithArgs(title, content, postID).QueryRow(ctx,
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt)
		return post, err
	}
}

func DeletePost(postID int) octobe.NoResultHandler[pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) error {
		query := newQuery(`DELETE FROM posts WHERE id = $1`)
		_, err := query.WithArgs(postID).Exec(ctx)
		return err
	}
}

// Comment operations
func CreateComment(postID, authorID int, content string) octobe.Handler[Comment, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Comment, error) {
		var comment Comment
		query := newQuery(`
			INSERT INTO comments (post_id, author_id, content)
			VALUES ($1, $2, $3)
			RETURNING id, post_id, author_id, content, created_at`)

		err := query.WithArgs(postID, authorID, content).QueryRow(ctx,
			&comment.ID, &comment.PostID, &comment.AuthorID,
			&comment.Content, &comment.CreatedAt)
		return comment, err
	}
}

func GetCommentsByPost(postID int) octobe.Handler[[]Comment, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) ([]Comment, error) {
		query := newQuery(`
			SELECT id, post_id, author_id, content, created_at
			FROM comments
			WHERE post_id = $1
			ORDER BY created_at ASC`)

		var comments []Comment
		err := query.WithArgs(postID).Query(ctx, func(rows pgx.Rows) error {
			for rows.Next() {
				var comment Comment
				if err := rows.Scan(&comment.ID, &comment.PostID, &comment.AuthorID,
					&comment.Content, &comment.CreatedAt); err != nil {
					return err
				}
				comments = append(comments, comment)
			}
			return rows.Err()
		})

		return comments, err
	}
}

// Tag operations
func EnsureTag(name string) octobe.Handler[Tag, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Tag, error) {
		var tag Tag
		query := newQuery(`
			INSERT INTO tags (name) VALUES ($1)
			ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
			RETURNING id, name`)

		err := query.WithArgs(name).QueryRow(ctx, &tag.ID, &tag.Name)
		return tag, err
	}
}

func AddTagToPost(postID, tagID int) octobe.NoResultHandler[pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) error {
		query := newQuery(`
			INSERT INTO post_tags (post_id, tag_id)
			VALUES ($1, $2)
			ON CONFLICT (post_id, tag_id) DO NOTHING`)

		_, err := query.WithArgs(postID, tagID).Exec(ctx)
		return err
	}
}

// Complex operations that demonstrate transaction usage
func CreatePostWithTags(title, content string, authorID int, tagNames []string) octobe.Handler[Post, pgx.QueryFactory] {
	return func(ctx context.Context, newQuery pgx.QueryFactory) (Post, error) {
		// This handler demonstrates multiple related operations
		// that should succeed or fail together

		// 1. Create the post
		var post Post
		query := newQuery(`
			INSERT INTO posts (title, content, author_id)
			VALUES ($1, $2, $3)
			RETURNING id, title, content, author_id, created_at, updated_at`)

		err := query.WithArgs(title, content, authorID).QueryRow(ctx,
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt)
		if err != nil {
			return post, fmt.Errorf("failed to create post: %w", err)
		}

		// 2. Create tags and associate them with the post
		for _, tagName := range tagNames {
			// Create or get existing tag
			var tagID int
			tagQuery := newQuery(`
				INSERT INTO tags (name) VALUES ($1)
				ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
				RETURNING id`)

			err = tagQuery.WithArgs(tagName).QueryRow(ctx, &tagID)
			if err != nil {
				return post, fmt.Errorf("failed to create tag %s: %w", tagName, err)
			}

			// Link tag to post
			linkQuery := newQuery(`
				INSERT INTO post_tags (post_id, tag_id)
				VALUES ($1, $2)
				ON CONFLICT (post_id, tag_id) DO NOTHING`)

			_, err = linkQuery.WithArgs(post.ID, tagID).Exec(ctx)
			if err != nil {
				return post, fmt.Errorf("failed to link tag %s to post: %w", tagName, err)
			}
		}

		return post, nil
	}
}

// Application service layer - demonstrates transaction usage
type BlogService struct {
	db pgx.Driver
}

func NewBlogService(db pgx.Driver) *BlogService {
	return &BlogService{db: db}
}

func (s *BlogService) CreateUserAndWelcomePost(ctx context.Context, username, email string) (*User, *Post, error) {
	var user User
	var post Post

	err := s.db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		var err error

		// Create user
		user, err = session.Execute(ctx, CreateUser(username, email))
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}

		// Create welcome post
		welcomeTitle := fmt.Sprintf("Welcome %s!", username)
		welcomeContent := fmt.Sprintf("Hello %s! Welcome to our blog platform. This is your first post!", username)

		post, err = session.Execute(ctx, CreatePost(welcomeTitle, welcomeContent, user.ID))
		if err != nil {
			return fmt.Errorf("failed to create welcome post: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return &user, &post, nil
}

func (s *BlogService) GetPostWithComments(ctx context.Context, postID int) (*Post, []Comment, error) {
	var post Post
	var comments []Comment

	err := s.db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		var err error

		post, err = session.Execute(ctx, GetPostWithAuthor(postID))
		if err != nil {
			return fmt.Errorf("failed to get post: %w", err)
		}

		comments, err = session.Execute(ctx, GetCommentsByPost(postID))
		if err != nil {
			return fmt.Errorf("failed to get comments: %w", err)
		}

		return nil
	})
	if err != nil {
		return nil, nil, err
	}

	return &post, comments, nil
}

func main() {
	// Get database URL from environment
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgresql://user:password@localhost:5432/blogdb?sslmode=disable"
		log.Printf("Using default database URL: %s", dsn)
		log.Println("Set DATABASE_URL environment variable to use a different database")
	}

	ctx := context.Background()

	// Initialize database
	db, err := octobe.New(pgx.OpenPool(ctx, dsn))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer func() {
		if err := db.Close(ctx); err != nil {
			log.Fatalf("Failed to close database: %v", err)
		}
	}()

	// Test connection
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to database successfully")

	// Create schema
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		return session.ExecuteNoResult(ctx, EnsureSchema())
	})
	if err != nil {
		log.Fatalf("Failed to create schema: %v", err)
	}
	log.Println("Database schema created")

	// Create service
	service := NewBlogService(db)

	// Demo: Create user and welcome post
	user, welcomePost, err := service.CreateUserAndWelcomePost(ctx, "alice", "alice@example.com")
	if err != nil {
		log.Fatalf("Failed to create user and welcome post: %v", err)
	}

	fmt.Printf("Created user: %s (ID: %d)\n", user.Username, user.ID)
	fmt.Printf("Created welcome post: %s (ID: %d)\n", welcomePost.Title, welcomePost.ID)

	// Demo: Create another user
	var bob User
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		bob, err = session.Execute(ctx, CreateUser("bob", "bob@example.com"))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to create user bob: %v", err)
	}
	fmt.Printf("Created user: %s (ID: %d)\n", bob.Username, bob.ID)

	// Demo: Create a blog post with tags
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		post, err := session.Execute(ctx, CreatePostWithTags(
			"Getting Started with Go",
			"Go is a fantastic programming language for backend development...",
			bob.ID,
			[]string{"go", "programming", "tutorial"}))
		if err != nil {
			return err
		}

		fmt.Printf("Created post with tags: %s (ID: %d)\n", post.Title, post.ID)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to create post with tags: %v", err)
	}

	// Demo: Add comments
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		comment, err := session.Execute(ctx, CreateComment(welcomePost.ID, bob.ID, "Welcome to the platform, Alice!"))
		if err != nil {
			return err
		}

		fmt.Printf("Created comment: %s (ID: %d)\n", comment.Content, comment.ID)
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to create comment: %v", err)
	}

	// Demo: Get post with comments
	post, comments, err := service.GetPostWithComments(ctx, welcomePost.ID)
	if err != nil {
		log.Fatalf("Failed to get post with comments: %v", err)
	}

	fmt.Printf("\n=== Post Details ===\n")
	fmt.Printf("Title: %s\n", post.Title)
	fmt.Printf("Author: %s\n", post.Author.Username)
	fmt.Printf("Created: %s\n", post.CreatedAt.Format(time.RFC3339))
	fmt.Printf("Content: %s\n", post.Content)

	fmt.Printf("\n=== Comments ===\n")
	for _, comment := range comments {
		fmt.Printf("Comment ID %d: %s\n", comment.ID, comment.Content)
	}

	// Demo: Get all posts by alice
	err = db.RunInTransaction(ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		posts, err := session.Execute(ctx, GetPostsByAuthor(user.ID))
		if err != nil {
			return err
		}

		fmt.Printf("\n=== Posts by %s ===\n", user.Username)
		for _, p := range posts {
			fmt.Printf("- %s (created: %s)\n", p.Title, p.CreatedAt.Format("2006-01-02 15:04:05"))
		}
		return nil
	})
	if err != nil {
		log.Fatalf("Failed to get posts by author: %v", err)
	}

	fmt.Println("\nBlog demo completed successfully!")
}

// Note: This example uses interface{} for simplicity in type parameters.
// In real applications, you would import and use the specific driver types.

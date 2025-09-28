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

	"github.com/Kansuler/octobe/v3"
	"github.com/Kansuler/octobe/v3/driver/postgres"
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
func CreateSchema() octobe.Handler[octobe.Void, postgres.Builder] {
	return func(builder postgres.Builder) (octobe.Void, error) {
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

		query := builder(schema)
		_, err := query.Exec()
		return nil, err
	}
}

// User operations
func CreateUser(username, email string) octobe.Handler[User, postgres.Builder] {
	return func(builder postgres.Builder) (User, error) {
		var user User
		query := builder(`
			INSERT INTO users (username, email)
			VALUES ($1, $2)
			RETURNING id, username, email, created_at`)

		err := query.Arguments(username, email).QueryRow(
			&user.ID, &user.Username, &user.Email, &user.CreatedAt)
		return user, err
	}
}

func GetUserByID(id int) octobe.Handler[User, postgres.Builder] {
	return func(builder postgres.Builder) (User, error) {
		var user User
		query := builder(`
			SELECT id, username, email, created_at
			FROM users
			WHERE id = $1`)

		err := query.Arguments(id).QueryRow(
			&user.ID, &user.Username, &user.Email, &user.CreatedAt)
		return user, err
	}
}

func GetUserByUsername(username string) octobe.Handler[User, postgres.Builder] {
	return func(builder postgres.Builder) (User, error) {
		var user User
		query := builder(`
			SELECT id, username, email, created_at
			FROM users
			WHERE username = $1`)

		err := query.Arguments(username).QueryRow(
			&user.ID, &user.Username, &user.Email, &user.CreatedAt)
		return user, err
	}
}

// Post operations
func CreatePost(title, content string, authorID int) octobe.Handler[Post, postgres.Builder] {
	return func(builder postgres.Builder) (Post, error) {
		var post Post
		query := builder(`
			INSERT INTO posts (title, content, author_id)
			VALUES ($1, $2, $3)
			RETURNING id, title, content, author_id, created_at, updated_at`)

		err := query.Arguments(title, content, authorID).QueryRow(
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt)
		return post, err
	}
}

func GetPostWithAuthor(postID int) octobe.Handler[Post, postgres.Builder] {
	return func(builder postgres.Builder) (Post, error) {
		var post Post
		var author User

		query := builder(`
			SELECT
				p.id, p.title, p.content, p.author_id, p.created_at, p.updated_at,
				u.id, u.username, u.email, u.created_at
			FROM posts p
			JOIN users u ON p.author_id = u.id
			WHERE p.id = $1`)

		err := query.Arguments(postID).QueryRow(
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt,
			&author.ID, &author.Username, &author.Email, &author.CreatedAt)

		if err == nil {
			post.Author = &author
		}
		return post, err
	}
}

func GetPostsByAuthor(authorID int) octobe.Handler[[]Post, postgres.Builder] {
	return func(builder postgres.Builder) ([]Post, error) {
		query := builder(`
			SELECT id, title, content, author_id, created_at, updated_at
			FROM posts
			WHERE author_id = $1
			ORDER BY created_at DESC`)

		var posts []Post
		err := query.Arguments(authorID).Query(func(rows postgres.Rows) error {
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

func UpdatePost(postID int, title, content string) octobe.Handler[Post, postgres.Builder] {
	return func(builder postgres.Builder) (Post, error) {
		var post Post
		query := builder(`
			UPDATE posts
			SET title = $1, content = $2, updated_at = NOW()
			WHERE id = $3
			RETURNING id, title, content, author_id, created_at, updated_at`)

		err := query.Arguments(title, content, postID).QueryRow(
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt)
		return post, err
	}
}

func DeletePost(postID int) octobe.Handler[octobe.Void, postgres.Builder] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`DELETE FROM posts WHERE id = $1`)
		_, err := query.Arguments(postID).Exec()
		return nil, err
	}
}

// Comment operations
func CreateComment(postID, authorID int, content string) octobe.Handler[Comment, postgres.Builder] {
	return func(builder postgres.Builder) (Comment, error) {
		var comment Comment
		query := builder(`
			INSERT INTO comments (post_id, author_id, content)
			VALUES ($1, $2, $3)
			RETURNING id, post_id, author_id, content, created_at`)

		err := query.Arguments(postID, authorID, content).QueryRow(
			&comment.ID, &comment.PostID, &comment.AuthorID,
			&comment.Content, &comment.CreatedAt)
		return comment, err
	}
}

func GetCommentsByPost(postID int) octobe.Handler[[]Comment, postgres.Builder] {
	return func(builder postgres.Builder) ([]Comment, error) {
		query := builder(`
			SELECT id, post_id, author_id, content, created_at
			FROM comments
			WHERE post_id = $1
			ORDER BY created_at ASC`)

		var comments []Comment
		err := query.Arguments(postID).Query(func(rows postgres.Rows) error {
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
func CreateTag(name string) octobe.Handler[Tag, postgres.Builder] {
	return func(builder postgres.Builder) (Tag, error) {
		var tag Tag
		query := builder(`
			INSERT INTO tags (name) VALUES ($1)
			ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
			RETURNING id, name`)

		err := query.Arguments(name).QueryRow(&tag.ID, &tag.Name)
		return tag, err
	}
}

func AddTagToPost(postID, tagID int) octobe.Handler[octobe.Void, postgres.Builder] {
	return func(builder postgres.Builder) (octobe.Void, error) {
		query := builder(`
			INSERT INTO post_tags (post_id, tag_id)
			VALUES ($1, $2)
			ON CONFLICT (post_id, tag_id) DO NOTHING`)

		_, err := query.Arguments(postID, tagID).Exec()
		return nil, err
	}
}

// Complex operations that demonstrate transaction usage
func CreatePostWithTags(title, content string, authorID int, tagNames []string) octobe.Handler[Post, postgres.Builder] {
	return func(builder postgres.Builder) (Post, error) {
		// This handler demonstrates multiple related operations
		// that should succeed or fail together

		// 1. Create the post
		var post Post
		query := builder(`
			INSERT INTO posts (title, content, author_id)
			VALUES ($1, $2, $3)
			RETURNING id, title, content, author_id, created_at, updated_at`)

		err := query.Arguments(title, content, authorID).QueryRow(
			&post.ID, &post.Title, &post.Content, &post.AuthorID,
			&post.CreatedAt, &post.UpdatedAt)
		if err != nil {
			return post, fmt.Errorf("failed to create post: %w", err)
		}

		// 2. Create tags and associate them with the post
		for _, tagName := range tagNames {
			// Create or get existing tag
			var tagID int
			tagQuery := builder(`
				INSERT INTO tags (name) VALUES ($1)
				ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name
				RETURNING id`)

			err = tagQuery.Arguments(tagName).QueryRow(&tagID)
			if err != nil {
				return post, fmt.Errorf("failed to create tag %s: %w", tagName, err)
			}

			// Link tag to post
			linkQuery := builder(`
				INSERT INTO post_tags (post_id, tag_id)
				VALUES ($1, $2)
				ON CONFLICT (post_id, tag_id) DO NOTHING`)

			_, err = linkQuery.Arguments(post.ID, tagID).Exec()
			if err != nil {
				return post, fmt.Errorf("failed to link tag %s to post: %w", tagName, err)
			}
		}

		return post, nil
	}
}

// Application service layer - demonstrates transaction usage
type BlogService struct {
	db postgres.PGXDriver
}

func NewBlogService(db postgres.PGXDriver) *BlogService {
	return &BlogService{db: db}
}

func (s *BlogService) CreateUserAndWelcomePost(ctx context.Context, username, email string) (*User, *Post, error) {
	var user User
	var post Post

	err := s.db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		var err error

		// Create user
		user, err = octobe.Execute(session, CreateUser(username, email))
		if err != nil {
			return fmt.Errorf("failed to create user: %w", err)
		}

		// Create welcome post
		welcomeTitle := fmt.Sprintf("Welcome %s!", username)
		welcomeContent := fmt.Sprintf("Hello %s! Welcome to our blog platform. This is your first post!", username)

		post, err = octobe.Execute(session, CreatePost(welcomeTitle, welcomeContent, user.ID))
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

	err := s.db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		var err error

		post, err = octobe.Execute(session, GetPostWithAuthor(postID))
		if err != nil {
			return fmt.Errorf("failed to get post: %w", err)
		}

		comments, err = octobe.Execute(session, GetCommentsByPost(postID))
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
	dsn := os.Getenv("DSN")
	if dsn == "" {
		dsn = "postgresql://user:password@localhost:5432/blogdb?sslmode=disable"
		log.Printf("Using default DSN: %s", dsn)
		log.Println("Set DATABASE_URL environment variable to use a different database")
	}

	ctx := context.Background()

	// Initialize database
	db, err := octobe.New(postgres.OpenPGXPool(ctx, dsn))
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close(ctx)

	// Test connection
	if err := db.Ping(ctx); err != nil {
		log.Fatalf("Failed to ping database: %v", err)
	}
	log.Println("Connected to database successfully")

	// Create schema
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, CreateSchema())
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
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		bob, err = octobe.Execute(session, CreateUser("bob", "bob@example.com"))
		return err
	})
	if err != nil {
		log.Fatalf("Failed to create user bob: %v", err)
	}
	fmt.Printf("Created user: %s (ID: %d)\n", bob.Username, bob.ID)

	// Demo: Create a blog post with tags
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		post, err := octobe.Execute(session, CreatePostWithTags(
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
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		comment, err := octobe.Execute(session, CreateComment(welcomePost.ID, bob.ID, "Welcome to the platform, Alice!"))
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
	err = db.StartTransaction(ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		posts, err := octobe.Execute(session, GetPostsByAuthor(user.ID))
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

package integration

import (
	"context"
	"database/sql"
	"github.com/Kansuler/octobe"
	"github.com/Kansuler/octobe/example/database"
	"github.com/stretchr/testify/suite"
	"os"
	"testing"

	// This import only relates to database connection
	_ "github.com/jackc/pgx/v4/stdlib"
)

// Open will start a database connection and ping to confirm that it works
func open(con string) (db *sql.DB, err error) {
	// The `sql.Open` function opens a new `*sql.DB` instance. We specify the driver name
	// and the URI for our database. Here, we're using a Postgres URI
	db, err = sql.Open("pgx", con)
	if err != nil {
		return
	}

	// To verify the connection to our database instance, we can call the `Ping`
	// method. If no error is returned, we can assume a successful connection
	err = db.Ping()
	return
}

type PostgresTestSuite struct {
	suite.Suite
	db *sql.DB
}

func (suite *PostgresTestSuite) SetupSuite() {
	var err error
	suite.db, err = open(os.Getenv("DATABASE_CONNECTION_STRING"))
	if err != nil {
		suite.T().Fatalf("Failed to open database: %s", err)
	}

	ob := octobe.New(suite.db)
	scheme := ob.Begin(context.Background())
	seg := scheme.Segment(`
		CREATE TABLE IF NOT EXISTS products (
			id serial PRIMARY KEY,
			name varchar(255) NOT NULL
		);
	`)

	_, err = seg.Exec()
	if err != nil {
		suite.T().Fatalf("Failed to create table: %s", err)
	}
}

func (suite *PostgresTestSuite) Test() {
	suite.T().Run("TestBeginInsert", func(t *testing.T) {
		h := database.Handler{}
		ob := octobe.New(suite.db)
		scheme := ob.Begin(context.Background())
		p1 := database.Product{
			Name: "Product 1",
		}
		err := scheme.Handle(h.InsertProduct(&p1))
		suite.NoError(err)
		suite.NotEqual(0, p1.ID)
		suite.Equal("Product 1", p1.Name)

		p2 := database.Product{
			Name: "Product 2",
		}
		err = scheme.Handle(h.InsertProduct(&p2))
		suite.NoError(err)
		suite.NotEqual(0, p2.ID)
		suite.Equal("Product 2", p2.Name)
	})

	suite.T().Run("TestBeginSelect", func(t *testing.T) {
		h := database.Handler{}
		ob := octobe.New(suite.db)
		scheme := ob.Begin(context.Background())

		p1 := database.Product{
			Name: "Product To Be Selected",
		}
		err := scheme.Handle(h.InsertProduct(&p1))
		suite.NoError(err)
		suite.NotEqual(0, p1.ID)
		suite.Equal("Product To Be Selected", p1.Name)

		var p2 database.Product
		err = scheme.Handle(h.ProductByID(p1.ID, &p2))
		suite.NoError(err)
		suite.Equal(p1.ID, p2.ID)
		suite.Equal(p1.Name, p2.Name)
	})

	suite.T().Run("TestBeginUpdate", func(t *testing.T) {
		h := database.Handler{}
		ob := octobe.New(suite.db)
		scheme := ob.Begin(context.Background())

		p1 := database.Product{
			Name: "Product Before Update",
		}

		err := scheme.Handle(h.InsertProduct(&p1))
		suite.NoError(err)
		suite.NotEqual(0, p1.ID)
		suite.Equal("Product Before Update", p1.Name)

		p1.Name = "Product After Update"

		err = scheme.Handle(h.UpdateProduct(&p1))
		suite.NoError(err)
		suite.NotEqual(0, p1.ID)
		suite.Equal("Product After Update", p1.Name)

		var p2 database.Product
		err = scheme.Handle(h.ProductByID(p1.ID, &p2))
		suite.NoError(err)
		suite.Equal(p1.ID, p2.ID)
		suite.Equal(p1.Name, p2.Name)
	})

	suite.T().Run("TestBeginTxInsert", func(t *testing.T) {
		h := database.Handler{}
		ob := octobe.New(suite.db)
		scheme, err := ob.BeginTx(context.Background())
		if err != nil {
			suite.T().Fatalf("Failed to begin transaction: %s", err)
		}

		p1 := database.Product{
			Name: "Product Before Update",
		}

		err = scheme.Handle(h.InsertProduct(&p1))
		suite.NoError(err)
		suite.NotEqual(0, p1.ID)
		suite.Equal("Product Before Update", p1.Name)

		err = scheme.Commit()
		if err != nil {
			suite.T().Fatalf("Failed to commit transaction: %s", err)
		}

		scheme = ob.Begin(context.Background())
		var p2 database.Product
		err = scheme.Handle(h.ProductByID(p1.ID, &p2))
		suite.NoError(err)
		suite.Equal(p1.ID, p2.ID)
		suite.Equal(p1.Name, p2.Name)
	})

	suite.T().Run("TestWatchTransaction", func(t *testing.T) {
		h := database.Handler{}
		ob := octobe.New(suite.db)
		err := ob.WatchTransaction(context.Background(), func(scheme *octobe.Scheme) error {
			p1 := database.Product{
				Name: "Product Before Update",
			}

			err := scheme.Handle(h.InsertProduct(&p1))
			suite.NoError(err)
			suite.NotEqual(0, p1.ID)
			suite.Equal("Product Before Update", p1.Name)

			var p2 database.Product
			err = scheme.Handle(h.ProductByID(p1.ID, &p2))
			suite.NoError(err)
			suite.Equal(p1.ID, p2.ID)
			suite.Equal(p1.Name, p2.Name)

			return nil
		})
		suite.NoError(err)
	})
}

func TestPostgres(t *testing.T) {
	suite.Run(t, new(PostgresTestSuite))
}

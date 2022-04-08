package database

import (
	"database/sql"

	"github.com/Kansuler/octobe"
)

// Database interface that implements the databases, with this you can mock Database.
type Database interface {
	InsertProduct(*Product) octobe.Handler
	UpdateProduct(*Product) octobe.Handler
	ProductByID(*Product) octobe.Handler
	Products(*[]Product) octobe.Handler
}

// Handler implements the database interface
type Handler struct{}

// Product is an example database model
type Product struct {
	ID   int64  `json:"id"`
	Name string `json:"name"`
}

// InsertProduct will take a pointer of a product, and insert it
func (Handler) InsertProduct(p *Product) octobe.Handler {
	return func(scheme *octobe.Scheme) error {
		seg := scheme.Segment(`
			INSERT INTO
				products(name)
			VALUES($1)
			RETURNING id
		`)

		seg.Arguments(p.Name)

		return seg.QueryRow(&p.ID)
	}
}

// UpdateProduct will take a pointer and update the fields
func (Handler) UpdateProduct(p *Product) octobe.Handler {
	return func(scheme *octobe.Scheme) error {
		seg := scheme.Segment(`
			UPDATE
				products
			SET
				name = $2
			WHERE
				id = $1
		`)

		seg.Arguments(p.ID, p.Name)

		_, err := seg.Exec()
		return err
	}
}

// ProductByID will take an id, and a pointer to scan into
func (Handler) ProductByID(id int64, p *Product) octobe.Handler {
	return func(scheme *octobe.Scheme) error {
		seg := scheme.Segment(`
			SELECT
				id,
				name
			FROM
				products
			WHERE
				id = $1
		`)

		seg.Arguments(id)

		return seg.QueryRow(&p.ID, &p.Name)
	}
}

// Products will take a pointer to append Products into
func (Handler) Products(result *[]Product) octobe.Handler {
	return func(scheme *octobe.Scheme) (err error) {
		seg := scheme.Segment(`
			SELECT
				id,
				name
			FROM
				products
		`)

		err = seg.Query(func(rows *sql.Rows) error {
			for rows.Next() {
				var p Product
				err = rows.Scan(
					&p.ID,
					&p.Name,
				)

				// Will stop function, and return err from seg.Query
				if err != nil {
					return err
				}

				*result = append(*result, p)
			}
			return nil
		})

		return
	}
}

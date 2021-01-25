package database

import (
	"database/sql"
	"github.com/Kansuler/octobe"
)

// Product is an example database model
type Product struct {
	ID   string `json:"id"`
	Name string `json:"name"`
}

// InsertProduct will take a pointer of a product, and insert it
func InsertProduct(p *Product) octobe.Handler {
	return func(scheme octobe.Scheme) error {
		seg := scheme.NewSegment(`
			INSERT INTO
				products(name)
			VALUES($1)
			RETURNING id
		`)

		seg.Arguments(p.Name)

		return seg.Insert(&p.ID)
	}
}

// UpdateProduct will take a pointer and update the fields
func UpdateProduct(p *Product) octobe.Handler {
	return func(scheme octobe.Scheme) error {
		seg := scheme.NewSegment(`
			UPDATE
				products
			SET
				name = $2
			WHERE
				id = $1
		`)

		seg.Arguments(p.ID, p.Name)

		return seg.Exec()
	}
}

// ProductByID will take an id, and a pointer to scan into
func ProductByID(id string, p *Product) octobe.Handler {
	return func(scheme octobe.Scheme) error {
		seg := scheme.NewSegment(`
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
func Products(result *[]Product) (handler octobe.Handler) {
	return func(scheme octobe.Scheme) (err error) {
		seg := scheme.NewSegment(`
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

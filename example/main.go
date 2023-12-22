package main

import (
	"context"
	"github.com/Kansuler/octobe/example/query"
	"github.com/Kansuler/octobe/v2"
	"github.com/Kansuler/octobe/v2/driver/postgres"
	"github.com/go-chi/chi/v5"
	"net/http"
	"os"
)

type Container struct {
	Postgres postgres.Driver
}

func main() {
	ctx := context.Background()
	cnt := Container{}
	var err error
	dsn := os.Getenv("DSN")
	if dsn == "" {
		panic("DSN is not set")
	}

	cnt.Postgres, err = octobe.New(postgres.Open(ctx, dsn))
	if err != nil {
		panic(err)
	}

	session, err := cnt.Postgres.Begin(ctx)
	if err != nil {
		panic(err)
	}

	_, err = postgres.Execute(session, query.Migration())
	if err != nil {
		panic(err)
	}

	err = router(cnt)
	if err != nil {
		panic(err)
	}
}

func router(cnt Container) error {
	r := chi.NewRouter()

	// Postgres
	r.Get("/postgres/product/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		session, err := cnt.Postgres.Begin(r.Context())
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		product, err := postgres.Execute(session, query.ProductByName(name))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		_, err = w.Write([]byte(product.Name))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	})
	r.Post("/postgres/product/{name}", func(w http.ResponseWriter, r *http.Request) {
		name := chi.URLParam(r, "name")
		session, err := cnt.Postgres.Begin(r.Context(), postgres.WithTransaction(postgres.TxOptions{}))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		defer session.WatchRollback(func() error {
			return err
		})

		product, err := postgres.Execute(session, query.AddProduct(name))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		err = session.Commit()
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}

		_, err = w.Write([]byte(product.Name))
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			w.Write([]byte(err.Error()))
			return
		}
	})

	return http.ListenAndServe(":8080", r)
}

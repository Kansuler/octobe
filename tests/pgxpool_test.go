package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v3"
	"github.com/Kansuler/octobe/v3/driver/postgres"
	"github.com/stretchr/testify/suite"
)

const pgxPoolProductsTable = "pgxpool_integration_products"

type PGXPoolIntegrationSuite struct {
	suite.Suite

	ctx context.Context
	db  postgres.PGXPoolDriver
}

func TestPGXPoolIntegrationSuite(t *testing.T) {
	suite.Run(t, new(PGXPoolIntegrationSuite))
}

func (s *PGXPoolIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()
	s.db = openPGXPoolWithRetry(s.T(), s.ctx, integrationDSN(s.T()))

	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, migrateProducts(pgxPoolProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXPoolIntegrationSuite) SetupTest() {
	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, truncateProducts(pgxPoolProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXPoolIntegrationSuite) TearDownSuite() {
	if s.db == nil {
		return
	}

	_ = s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, dropProducts(pgxPoolProductsTable))
	})
	s.Require().NoError(s.db.Close(s.ctx))
}

func (s *PGXPoolIntegrationSuite) TestStartTransactionCommits() {
	name := "pgxpool committed product"
	var created integrationProduct

	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		var err error
		created, err = octobe.Execute(session, createProduct(pgxPoolProductsTable, name))
		return err
	})
	s.Require().NoError(err)

	loaded, err := s.findPGXPoolProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXPoolIntegrationSuite) TestStartTransactionRollsBackOnError() {
	name := "pgxpool rolled back product"
	expectedErr := errors.New("force rollback")

	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err := octobe.Execute(session, createProduct(pgxPoolProductsTable, name))
		if err != nil {
			return err
		}
		return expectedErr
	})
	s.ErrorIs(err, expectedErr)

	products, err := s.findPGXPoolProductsByName(name)
	s.Require().NoError(err)
	s.Empty(products)
}

func (s *PGXPoolIntegrationSuite) TestManualTransactionCommits() {
	name := "pgxpool manual commit product"

	session, err := s.db.Begin(s.ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback() }()

	created, err := octobe.Execute(session, createProduct(pgxPoolProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Commit())

	loaded, err := s.findPGXPoolProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXPoolIntegrationSuite) TestManualTransactionRollsBack() {
	name := "pgxpool manual rollback product"

	session, err := s.db.Begin(s.ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback() }()

	_, err = octobe.Execute(session, createProduct(pgxPoolProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Rollback())

	products, err := s.findPGXPoolProductsByName(name)
	s.Require().NoError(err)
	s.Empty(products)
}

func (s *PGXPoolIntegrationSuite) findPGXPoolProduct(id int) (integrationProduct, error) {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	return octobe.Execute(session, productByID(pgxPoolProductsTable, id))
}

func (s *PGXPoolIntegrationSuite) findPGXPoolProductsByName(name string) ([]integrationProduct, error) {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	return octobe.Execute(session, productsByName(pgxPoolProductsTable, name))
}

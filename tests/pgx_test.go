package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v3"
	"github.com/Kansuler/octobe/v3/driver/postgres"
	"github.com/stretchr/testify/suite"
)

const pgxProductsTable = "pgx_integration_products"

type PGXIntegrationSuite struct {
	suite.Suite

	ctx context.Context
	db  postgres.PGXDriver
}

func TestPGXIntegrationSuite(t *testing.T) {
	suite.Run(t, new(PGXIntegrationSuite))
}

func (s *PGXIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()
	s.db = openPGXWithRetry(s.T(), s.ctx, integrationDSN(s.T()))

	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, migrateProducts(pgxProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXIntegrationSuite) SetupTest() {
	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, truncateProducts(pgxProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXIntegrationSuite) TearDownSuite() {
	if s.db == nil {
		return
	}

	_ = s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		return octobe.ExecuteVoid(session, dropProducts(pgxProductsTable))
	})
	s.Require().NoError(s.db.Close(s.ctx))
}

func (s *PGXIntegrationSuite) TestStartTransactionCommits() {
	name := "pgx committed product"
	var created integrationProduct

	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		var err error
		created, err = octobe.Execute(session, createProduct(pgxProductsTable, name))
		return err
	})
	s.Require().NoError(err)

	loaded, err := s.findPGXProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXIntegrationSuite) TestStartTransactionRollsBackOnError() {
	name := "pgx rolled back product"
	expectedErr := errors.New("force rollback")

	err := s.db.StartTransaction(s.ctx, func(session octobe.BuilderSession[postgres.Builder]) error {
		_, err := octobe.Execute(session, createProduct(pgxProductsTable, name))
		if err != nil {
			return err
		}
		return expectedErr
	})
	s.ErrorIs(err, expectedErr)

	products, err := s.findPGXProductsByName(name)
	s.Require().NoError(err)
	s.Empty(products)
}

func (s *PGXIntegrationSuite) TestManualTransactionCommits() {
	name := "pgx manual commit product"

	session, err := s.db.BeginTx(s.ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback() }()

	created, err := octobe.Execute(session, createProduct(pgxProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Commit())

	loaded, err := s.findPGXProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXIntegrationSuite) TestManualTransactionRollsBack() {
	name := "pgx manual rollback product"

	session, err := s.db.BeginTx(s.ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback() }()

	_, err = octobe.Execute(session, createProduct(pgxProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Rollback())

	products, err := s.findPGXProductsByName(name)
	s.Require().NoError(err)
	s.Empty(products)
}

func (s *PGXIntegrationSuite) findPGXProduct(id int) (integrationProduct, error) {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close()) }()
	return octobe.Execute(session, productByID(pgxProductsTable, id))
}

func (s *PGXIntegrationSuite) findPGXProductsByName(name string) ([]integrationProduct, error) {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close()) }()
	return octobe.Execute(session, productsByName(pgxProductsTable, name))
}

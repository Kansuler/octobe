package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/postgres"
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

	err := s.db.StartTransaction(s.ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		return session.ExecuteVoid(s.ctx, migrateProducts(pgxPoolProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXPoolIntegrationSuite) SetupTest() {
	err := s.db.StartTransaction(s.ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		return session.ExecuteVoid(s.ctx, truncateProducts(pgxPoolProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXPoolIntegrationSuite) TearDownSuite() {
	if s.db == nil {
		return
	}

	_ = s.db.StartTransaction(s.ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		return session.ExecuteVoid(s.ctx, dropProducts(pgxPoolProductsTable))
	})
	s.Require().NoError(s.db.Close(s.ctx))
}

func (s *PGXPoolIntegrationSuite) TestStartTransactionCommits() {
	name := "pgxpool committed product"
	var created integrationProduct

	err := s.db.StartTransaction(s.ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		var err error
		created, err = session.Execute(s.ctx, createProduct(pgxPoolProductsTable, name))
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

	err := s.db.StartTransaction(s.ctx, func(session *octobe.ManagedSession[postgres.Builder]) error {
		_, err := session.Execute(s.ctx, createProduct(pgxPoolProductsTable, name))
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

	session, err := s.db.BeginTx(s.ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback(s.ctx) }()

	created, err := session.Execute(s.ctx, createProduct(pgxPoolProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Commit(s.ctx))

	loaded, err := s.findPGXPoolProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXPoolIntegrationSuite) TestManualTransactionRollsBack() {
	name := "pgxpool manual rollback product"

	session, err := s.db.BeginTx(s.ctx, postgres.WithPGXTxOptions(postgres.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback(s.ctx) }()

	_, err = session.Execute(s.ctx, createProduct(pgxPoolProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Rollback(s.ctx))

	products, err := s.findPGXPoolProductsByName(name)
	s.Require().NoError(err)
	s.Empty(products)
}

func (s *PGXPoolIntegrationSuite) TestNonTransactionalSessionPinsConnection() {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close(s.ctx)) }()

	first, err := session.Execute(s.ctx, backendPID())
	s.Require().NoError(err)

	second, err := session.Execute(s.ctx, backendPID())
	s.Require().NoError(err)
	s.Equal(first, second)
}

func (s *PGXPoolIntegrationSuite) findPGXPoolProduct(id int) (integrationProduct, error) {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close(s.ctx)) }()
	return session.Execute(s.ctx, productByID(pgxPoolProductsTable, id))
}

func (s *PGXPoolIntegrationSuite) findPGXPoolProductsByName(name string) ([]integrationProduct, error) {
	session, err := s.db.Begin(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close(s.ctx)) }()
	return session.Execute(s.ctx, productsByName(pgxPoolProductsTable, name))
}

package tests

import (
	"context"
	"errors"
	"testing"

	"github.com/Kansuler/octobe/v4"
	"github.com/Kansuler/octobe/v4/driver/pgx"
	"github.com/stretchr/testify/suite"
)

const pgxProductsTable = "pgx_integration_products"

type PGXIntegrationSuite struct {
	suite.Suite

	ctx context.Context
	db  pgx.PGXDriver
}

func TestPGXIntegrationSuite(t *testing.T) {
	suite.Run(t, new(PGXIntegrationSuite))
}

func (s *PGXIntegrationSuite) SetupSuite() {
	s.ctx = context.Background()
	s.db = openPGXWithRetry(s.T(), s.ctx, integrationDSN(s.T()))

	err := s.db.RunInTransaction(s.ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		return session.ExecuteNoResult(s.ctx, migrateProducts(pgxProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXIntegrationSuite) SetupTest() {
	err := s.db.RunInTransaction(s.ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		return session.ExecuteNoResult(s.ctx, truncateProducts(pgxProductsTable))
	})
	s.Require().NoError(err)
}

func (s *PGXIntegrationSuite) TearDownSuite() {
	if s.db == nil {
		return
	}

	_ = s.db.RunInTransaction(s.ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		return session.ExecuteNoResult(s.ctx, dropProducts(pgxProductsTable))
	})
	s.Require().NoError(s.db.Close(s.ctx))
}

func (s *PGXIntegrationSuite) TestRunInTransactionCommits() {
	name := "pgx committed product"
	var created integrationProduct

	err := s.db.RunInTransaction(s.ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		var err error
		created, err = session.Execute(s.ctx, createProduct(pgxProductsTable, name))
		return err
	})
	s.Require().NoError(err)

	loaded, err := s.findPGXProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXIntegrationSuite) TestRunInTransactionRollsBackOnError() {
	name := "pgx rolled back product"
	expectedErr := errors.New("force rollback")

	err := s.db.RunInTransaction(s.ctx, func(session *octobe.SessionManaged[pgx.QueryFactory]) error {
		_, err := session.Execute(s.ctx, createProduct(pgxProductsTable, name))
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

	session, err := s.db.Transaction(s.ctx, pgx.WithPGXTxOptions(pgx.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback(s.ctx) }()

	created, err := session.Execute(s.ctx, createProduct(pgxProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Commit(s.ctx))

	loaded, err := s.findPGXProduct(created.ID)
	s.Require().NoError(err)
	s.Equal(created, loaded)
}

func (s *PGXIntegrationSuite) TestManualTransactionRollsBack() {
	name := "pgx manual rollback product"

	session, err := s.db.Transaction(s.ctx, pgx.WithPGXTxOptions(pgx.PGXTxOptions{}))
	s.Require().NoError(err)
	defer func() { _ = session.Rollback(s.ctx) }()

	_, err = session.Execute(s.ctx, createProduct(pgxProductsTable, name))
	s.Require().NoError(err)
	s.Require().NoError(session.Rollback(s.ctx))

	products, err := s.findPGXProductsByName(name)
	s.Require().NoError(err)
	s.Empty(products)
}

func (s *PGXIntegrationSuite) findPGXProduct(id int) (integrationProduct, error) {
	session, err := s.db.Session(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close(s.ctx)) }()
	return session.Execute(s.ctx, productByID(pgxProductsTable, id))
}

func (s *PGXIntegrationSuite) findPGXProductsByName(name string) ([]integrationProduct, error) {
	session, err := s.db.Session(s.ctx)
	s.Require().NoError(err)
	defer func() { s.Require().NoError(session.Close(s.ctx)) }()
	return session.Execute(s.ctx, productsByName(pgxProductsTable, name))
}

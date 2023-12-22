package firestore

import (
	sdk "cloud.google.com/go/firestore/apiv1"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"context"
	"errors"
	"fmt"
	"github.com/Kansuler/octobe/v2"
	"github.com/googleapis/gax-go/v2"
	"google.golang.org/api/option"
	"google.golang.org/api/transport"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"os"
	"time"
)

type Driver octobe.Driver[firestore, config, Builder]

// postgres holds the connection pool and default configuration for the postgres driver
type firestore struct {
	client *sdk.Client
	cfg    config
}

// config defined various configurations possible for the postgres driver
type config struct {
	projectID     string
	database      string
	transaction   bool
	readOnly      bool
	maxAttempts   int
	clientOptions []option.ClientOption
}

// Type check to make sure that the postgres driver implements the Octobe Driver interface
var _ octobe.Driver[firestore, config, Builder] = &firestore{}

// WithDatabase provides a database name to the firestore driver
func WithDatabase(database string) octobe.Option[config] {
	return func(c *config) {
		c.database = database
	}
}

func WithTransaction() octobe.Option[config] {
	return func(c *config) {
		c.transaction = true
	}
}

func WithoutTransaction() octobe.Option[config] {
	return func(c *config) {
		c.transaction = false
	}
}

func WithReadOnly() octobe.Option[config] {
	return func(c *config) {
		c.readOnly = true
	}
}

func WithMaxAttempts(maxAttempts int) octobe.Option[config] {
	return func(c *config) {
		c.maxAttempts = maxAttempts
	}
}

func WithClientOptions(opts ...option.ClientOption) octobe.Option[config] {
	return func(c *config) {
		c.clientOptions = append(c.clientOptions, opts...)
	}
}

// DetectProjectID is a sentinel value that instructs NewClient to detect the
// project ID. It is given in place of the projectID argument. NewClient will
// use the project ID from the given credentials or the default credentials
// (https://developers.google.com/accounts/docs/application-default-credentials)
// if no credentials were provided. When providing credentials, not all
// options will allow NewClient to extract the project ID. Specifically a JWT
// does not have the project ID encoded.
const DetectProjectID = "*detect-project-id*"

// DefaultDatabaseID is name of the default database
const DefaultDatabaseID = "(default)"

// emulatorCreds is an instance of grpc.PerRPCCredentials that will configure a
// client to act as an admin for the Firestore emulator. It always hardcodes
// the "authorization" metadata field to contain "Bearer owner", which the
// Firestore emulator accepts as valid admin credentials.
type emulatorCreds struct{}

func (ec emulatorCreds) GetRequestMetadata(ctx context.Context, uri ...string) (map[string]string, error) {
	return map[string]string{"authorization": "Bearer owner"}, nil
}

func (ec emulatorCreds) RequireTransportSecurity() bool {
	return false
}

func detectProjectID(ctx context.Context, opts ...option.ClientOption) (string, error) {
	creds, err := transport.Creds(ctx, opts...)
	if err != nil {
		return "", fmt.Errorf("fetching creds: %w", err)
	}
	if creds.ProjectID == "" {
		return "", errors.New("firestore: see the docs on DetectProjectID")
	}
	return creds.ProjectID, nil
}

// Open is a function that can be used for opening a new database connection, it should always return a driver with set
// signature of types for the local driver.
func Open(ctx context.Context, opts ...octobe.Option[config]) octobe.Open[firestore, config, Builder] {
	return func() (octobe.Driver[firestore, config, Builder], error) {
		cfg := config{
			projectID:     DetectProjectID,
			database:      DefaultDatabaseID,
			clientOptions: []option.ClientOption{},
		}
		for _, opt := range opts {
			opt(&cfg)
		}

		var clientOptions []option.ClientOption
		// If this environment variable is defined, configure the client to talk to the emulator.
		if addr := os.Getenv("FIRESTORE_EMULATOR_HOST"); addr != "" {
			conn, err := grpc.Dial(addr, grpc.WithInsecure(), grpc.WithPerRPCCredentials(emulatorCreds{}))
			if err != nil {
				return nil, fmt.Errorf("firestore: dialing address from env var FIRESTORE_EMULATOR_HOST: %s", err)
			}
			clientOptions = []option.ClientOption{option.WithGRPCConn(conn)}
			if cfg.projectID == DetectProjectID {
				cfg.projectID, _ = detectProjectID(ctx, clientOptions...)
				if cfg.projectID == "" {
					cfg.projectID = "dummy-emulator-firestore-project"
				}
			}
		}

		if cfg.database == "" {
			cfg.database = DefaultDatabaseID
		}

		cfg.clientOptions = append(cfg.clientOptions, clientOptions...)

		if cfg.projectID == DetectProjectID {
			detected, err := detectProjectID(ctx, cfg.clientOptions...)
			if err != nil {
				return nil, err
			}
			cfg.projectID = detected
		}

		vc, err := sdk.NewClient(ctx, cfg.clientOptions...)
		if err != nil {
			return nil, err
		}
		vc.SetGoogleClientInfo("octobe", "v2.0.0")
		return &firestore{
			client: vc,
			cfg:    cfg,
		}, nil
	}
}

// Begin will start a new session with the database, this will return a Session instance that can be used for handling
// queries. Options can be passed to the driver for specific configuration that overwrites the default configuration
// given at instantiation of the Octobe instance. If no options are passed, the default configuration will be used.
// If the default configuration is not set, the session will not be transactional.
func (d *firestore) Begin(ctx context.Context, opts ...octobe.Option[config]) (octobe.Session[Builder], error) {
	cfg := d.cfg
	for _, opt := range opts {
		opt(&cfg)
	}

	s := session{
		cfg:    cfg,
		client: d.client,
	}
	if cfg.transaction {
		var txOpts *pb.TransactionOptions
		if cfg.readOnly {
			txOpts = &pb.TransactionOptions{
				Mode: &pb.TransactionOptions_ReadOnly_{ReadOnly: &pb.TransactionOptions_ReadOnly{}},
			}
		}
		resp, err := d.client.BeginTransaction(ctx, &pb.BeginTransactionRequest{
			Database: d.cfg.database,
			Options:  txOpts,
		})
		if err != nil {
			return nil, err
		}
		s.txId = resp.Transaction
	}

	return &s, nil
}

// session is a struct that holds session context, a session should be considered a series of queries that are related
// to each other. A session can be transactional or non-transactional, if it is transactional, it will enforce the usage
// of commit and rollback. If it is non-transactional, it will not enforce the usage of commit and rollback.
// A session is not thread safe, it should only be used in one thread at a time.
type session struct {
	ctx       context.Context
	cfg       config
	txId      []byte
	writes    []*pb.Write
	client    *sdk.Client
	committed bool
}

// Type check to make sure that the session implements the Octobe Session interface
var _ octobe.Session[Builder] = &session{}

func sleep(ctx context.Context, dur time.Duration) error {
	switch err := gax.Sleep(ctx, dur); {
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, "context canceled")
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, "context deadline exceeded")
	default:
		return err
	}
}

// Commit will commit a transaction, this will only work if the session is transactional.
func (s *session) Commit() error {
	if s.txId == nil {
		return errors.New("cannot commit without transaction")
	}
	defer func() {
		s.committed = true
	}()

	var err error
	var txOpts *pb.TransactionOptions
	var backoff gax.Backoff
	for i := 0; i < s.cfg.maxAttempts; i++ {
		_, err := s.client.Commit(s.ctx, &pb.CommitRequest{
			Database:    s.cfg.database,
			Writes:      s.writes,
			Transaction: s.txId,
		})

		// If a read-write transaction returns Aborted, retry.
		// On success or other failures, return here.
		if s.cfg.readOnly || status.Code(err) != codes.Aborted {
			// According to the Firestore team, we should not roll back here
			// if err != nil. But spanner does.
			// See https://code.googlesource.com/gocloud/+/master/spanner/transaction.go#740.
			return err
		}

		if txOpts == nil {
			// txOpts can only be nil if is the first retry of a read-write transaction.
			// (It is only set here and in the body of "if t.readOnly" above.)
			// Mention the transaction ID in BeginTransaction so the service
			// knows it is a retry.
			txOpts = &pb.TransactionOptions{
				Mode: &pb.TransactionOptions_ReadWrite_{
					ReadWrite: &pb.TransactionOptions_ReadWrite{RetryTransaction: s.txId},
				},
			}
		}
		// Use exponential backoff to avoid contention with other running
		// transactions.
		if cerr := sleep(s.ctx, backoff.Pause()); cerr != nil {
			err = cerr
			break
		}

		// Reset state for the next attempt.
		s.writes = nil
	}

	// If we run out of retries, return the last error we saw (which should
	// be the Aborted from Commit, or a context error).
	if err != nil {
		return s.Rollback()
	}

	return err
}

// Rollback will rollback a transaction, this will only work if the session is transactional.
func (s *session) Rollback() error {
	if s.txId == nil {
		return errors.New("cannot rollback without transaction")
	}
	return s.client.Rollback(s.ctx, &pb.RollbackRequest{
		Database:    s.cfg.database,
		Transaction: s.txId,
	})
}

// WatchRollback will watch for a rollback, if the session is not committed, it will rollback the transaction.
func (s *session) WatchRollback(cb func() error) {
	if !s.committed {
		_ = s.Rollback()
		return
	}

	if err := cb(); err != nil {
		_ = s.Rollback()
	}
}

// Builder is a function signature that is used for building queries with postgres
type Builder func(collection string) Segment

// addTransactionalWrites will add a transactional write to the session, rather than executing it directly. The method
// is added to the segment if the session is transactional.
func (s *session) addTransactionalWrites(writes ...*pb.Write) {
	s.writes = append(s.writes, writes...)
}

// Builder will return a new builder for building queries
func (s *session) Builder() Builder {
	return func(collection string) Segment {
		seg := Segment{
			collection: collection,
		}

		if s.txId != nil {
			seg.transaction = s.addTransactionalWrites
		}

		return seg
	}
}

type Segment struct {
	transaction func(writes ...*pb.Write)
	collection  string
}

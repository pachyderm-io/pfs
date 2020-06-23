package gc

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jinzhu/gorm"
	"github.com/lib/pq"
	"github.com/pachyderm/pachyderm/src/client/pkg/errors"
	"github.com/pachyderm/pachyderm/src/server/pkg/backoff"
	"github.com/pachyderm/pachyderm/src/server/pkg/obj"
	"golang.org/x/sync/errgroup"
)

const (
	defaultPostgresHost = "localhost"
	defaultPostgresPort = 32228
)

// NewLocalDB creates a local database client.
// TODO The code for setting up a postgres client should be refactored into a db package similar to obj.
func NewLocalDB() (*gorm.DB, error) {
	db, err := gorm.Open("postgres", fmt.Sprintf("host=%s port=%d dbname=pgc user=pachyderm password=elephantastic sslmode=disable", defaultPostgresHost, defaultPostgresPort))
	if err != nil {
		return nil, err
	}
	// TODO Determine reasonable defaults.
	db.LogMode(false)
	db.DB().SetMaxOpenConns(3)
	db.DB().SetMaxIdleConns(2)
	if err := initializeDb(db); err != nil {
		return nil, err
	}
	return db, nil
}

// WithLocalDB creates a local database client for testing during the lifetime of
// the callback.
func WithLocalDB(f func(*gorm.DB) error) (retErr error) {
	db, err := NewLocalDB()
	if err != nil {
		return err
	}
	if err := clearData(db); err != nil {
		return err
	}
	defer func() {
		if err := clearData(db); retErr == nil {
			retErr = err
		}
	}()
	return f(db)
}

func clearData(db *gorm.DB) error {
	if err := db.Exec("DELETE FROM chunks *").Error; err != nil {
		return err
	}
	return db.Exec("DELETE FROM refs *").Error

}

// WithLocalGarbageCollector creates a local garbage collector client for testing during the lifetime of
// the callback.
func WithLocalGarbageCollector(f func(context.Context, obj.Client, Client) error, opts ...Option) error {
	return obj.WithLocalClient(func(objClient obj.Client) error {
		return WithLocalDB(func(db *gorm.DB) error {
			return WithGarbageCollector(objClient, db, func(ctx context.Context, client Client) error {
				return f(ctx, objClient, client)
			}, opts...)
		})
	})
}

// WithGarbageCollector creates a garbage collector client for testing during the lifetime of
// the callback.
func WithGarbageCollector(objClient obj.Client, db *gorm.DB, f func(context.Context, Client) error, opts ...Option) error {
	client, err := NewClient(db)
	if err != nil {
		return err
	}
	// TODO May want to pipe a real context through here.
	cancelCtx, cancel := context.WithCancel(context.Background())
	eg, gcContext := errgroup.WithContext(cancelCtx)
	eg.Go(func() error {
		return Run(gcContext, objClient, db, opts...)
	})
	eg.Go(func() error {
		defer cancel()
		return f(gcContext, client)
	})
	return eg.Wait()
}

func isRetriableError(err error) bool {
	if err, ok := err.(*pq.Error); ok {
		return err.Code.Class().Name() == "transaction_rollback"
	}
	return false
}

type statementFunc func(*gorm.DB) *gorm.DB

func runTransaction(ctx context.Context, db *gorm.DB, stmtFuncs []statementFunc) error {
	for {
		err := tryTransaction(ctx, db, stmtFuncs)
		if isRetriableError(err) {
			continue
		}
		return errors.Wrapf(err, "error running transaction")
	}
}

func tryTransaction(ctx context.Context, db *gorm.DB, stmtFuncs []statementFunc) error {
	txn := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
	// So we don't leak in case of a panic somewhere unexpected
	defer func() {
		if r := recover(); r != nil {
			txn.Rollback()
		}
	}()
	if err := txn.Error; err != nil {
		return err
	}
	for _, stmtFunc := range stmtFuncs {
		if err := stmtFunc(txn).Error; err != nil {
			txn.Rollback()
			return err
		}
	}
	if err := txn.Commit().Error; err != nil {
		txn.Rollback()
		return err
	}
	return nil
}

func retry(name string, f func() error) error {
	return backoff.RetryNotify(f, backoff.NewExponentialBackOff(), func(err error, _ time.Duration) error {
		fmt.Printf("%v: %v\n", name, err)
		return nil
	})
}

package dbutil

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

const (
	// DefaultMaxOpenConns is the default maximum number of open connections; if you change
	// this, also consider changing the default from the environment in
	// serviceenv.GlobalConfiguration.
	DefaultMaxOpenConns = 10
	// DefaultMaxIdleConns is the default number of idle database connections to maintain.  (2
	// comes from the default in database/sql.go.)
	DefaultMaxIdleConns = 2
	// DefaultConnMaxLifetime is the default maximum amount of time a connection may be reused
	// for.  Defaults to no maximum.
	DefaultConnMaxLifetime = 0
	// DefaultConnMaxIdleTime is the default maximum amount of time a connection may be idle.
	// Defaults to no maximum.
	DefaultConnMaxIdleTime = 0
)

type dbConfig struct {
	host            string
	port            int
	user, password  string
	name            string
	maxOpenConns    int
	maxIdleConns    int
	connMaxLifetime time.Duration
	connMaxIdleTime time.Duration
}

func newConfig(opts ...Option) *dbConfig {
	dbc := &dbConfig{
		maxOpenConns:    DefaultMaxOpenConns,
		maxIdleConns:    DefaultMaxIdleConns,
		connMaxLifetime: DefaultConnMaxLifetime,
		connMaxIdleTime: DefaultConnMaxIdleTime,
	}
	for _, opt := range opts {
		opt(dbc)
	}
	return dbc
}

func getDSN(dbc *dbConfig) string {
	fields := map[string]string{
		"sslmode":         "disable",
		"connect_timeout": "30",

		// https://github.com/lib/pq/issues/889
		"binary_parameters": "yes",
	}
	if dbc.host != "" {
		fields["host"] = dbc.host
	}
	if dbc.port != 0 {
		fields["port"] = strconv.Itoa(dbc.port)
	}
	if dbc.name != "" {
		fields["dbname"] = dbc.name
	}
	if dbc.user != "" {
		fields["user"] = dbc.user
	}
	if dbc.password != "" {
		fields["password"] = dbc.password
	}
	var dsnParts []string
	for k, v := range fields {
		dsnParts = append(dsnParts, k+"="+v)
	}
	return strings.Join(dsnParts, " ")
}

// GetDSN returns the string for connecting to the postgres instance with the
// parameters specified in 'opts'. This is needed because 'listen' operations
// are not supported in generic SQL libraries and they need to be run in a side
// session.
func GetDSN(opts ...Option) string {
	dbc := newConfig(opts...)
	return getDSN(dbc)
}

// NewDB creates a new DB.
func NewDB(opts ...Option) (*sqlx.DB, error) {
	dbc := newConfig(opts...)
	db, err := sqlx.Open("postgres", getDSN(dbc))
	if err != nil {
		return nil, err
	}
	if dbc.maxOpenConns != 0 {
		db.SetMaxOpenConns(dbc.maxOpenConns)
	}
	// Always set these; 0 does not mean "use the default", it means "use zero".
	db.SetMaxIdleConns(dbc.maxIdleConns)
	db.SetConnMaxLifetime(dbc.connMaxLifetime)
	db.SetConnMaxIdleTime(dbc.connMaxIdleTime)
	return db, nil
}

// Interface is the common interface exposed by *sqlx.Tx and *sqlx.DB
type Interface interface {
	sqlx.ExtContext
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

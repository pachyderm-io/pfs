package dbutil

import (
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
)

const (
	// DefaultMaxOpenConns is the argument passed to SetMaxOpenConns
	DefaultMaxOpenConns = 10
)

type dbConfig struct {
	host           string
	port           int
	user, password string
	name           string
	maxOpenConns   int
}

func newConfig(opts ...Option) *dbConfig {
	dbc := &dbConfig{
		maxOpenConns: DefaultMaxOpenConns,
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
	return db, nil
}

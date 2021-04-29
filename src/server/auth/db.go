package auth

import (
	"github.com/jmoiron/sqlx"
	"golang.org/x/net/context"
)

// CreateAuthTokensTable creates the SQL table that stores Pachyderm auth tokens
// TODO(acohen4): Decide on hash as primary key or unique column
// CreateTokensTable sets up the postgres table which tracks active clusters
func CreateAuthTokensTable(ctx context.Context, tx *sqlx.Tx) error {
	_, err := tx.ExecContext(ctx, `
CREATE TABLE IF NOT EXISTS auth.auth_tokens (
	token_hash VARCHAR(4096) PRIMARY KEY,
	subject VARCHAR(64) NOT NULL,
	expiration TIMESTAMP,
	created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX subject_index
ON auth.auth_tokens (subject);
`)
	return err
}

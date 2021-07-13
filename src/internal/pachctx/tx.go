package pachctx

import "github.com/jmoiron/sqlx"

// Tx is the type of a transaction in pachyderm.
// It also carries additional context scoped information like the user performing the action.
type Tx struct {
	SQLTx *sqlx.Tx
	User  string
}

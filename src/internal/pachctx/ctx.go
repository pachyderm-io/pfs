package pachctx

import "context"

var _ context.Context = &Ctx{}

// Ctx is context.Context composed with additional scoped information like the user perfoming the action
type Ctx struct {
	context.Context
	User string
}

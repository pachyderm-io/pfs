package httperr

import (
	"fmt"
	"net/http"
)

// HTTPError is an error that extends the built-in 'error' with an HTTP error
// code.  This is useful for functions that implement the core functionality of
// HTTP endpoints (and therefore specify the error type) but not logging, etc,
// which would be in the 'func(http.ResponseWriter, *http.Request)' handler.
type HTTPError struct {
	err  string // Not an interface--both 'err' and 'code' should be set or niether
	code int
}

func (h *HTTPError) Error() string {
	if h == nil {
		return ""
	}
	return h.err
}

// Code returns the HTTP error code associated with 'h'
func (h *HTTPError) Code() int {
	if h == nil {
		return http.StatusOK
	}
	return h.code
}

// PrettyPrintCode renders 'h' as "<error code> <description>", e.g. "200 OK"
func PrettyPrintCode(h *HTTPError) string {
	codeNumber := h.Code()
	codeText := http.StatusText(h.Code())
	return fmt.Sprintf("%d %s", codeNumber, codeText)
}

// New returns a new HTTPError where the HTTP error code is 'code' and
// the error message is based on 'formatStr' and 'args'
func New(code int, formatStr string, args ...interface{}) *HTTPError {
	return &HTTPError{
		code: code,
		err:  fmt.Sprintf(formatStr, args...),
	}
}

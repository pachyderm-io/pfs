package serviceenv

import (
	"bufio"
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"runtime/debug"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/pachyderm/pachyderm/src/server/pkg/uuid"
)

const bodyPrefixLength = 200
const cancellationsBeforeReset = 30

// This is a copy of an internal interface used by k8s.io/client-go when using
// a passed-in http.RoundTripper. We only need it in order to wrap client-go's
// RoundTripper with logging
type kubeRequestCanceler interface {
	CancelRequest(*http.Request)
}

// bufReadCloser is very similar to a bufio.Reader, but it wraps a ReadCloser
// (unlike bufio.Reader, which only wraps an io.Reader)
type bufReadCloser struct {
	*bufio.Reader
	closefunc func() error
}

// Close implements the corresponding method of io.Closer (and io.ReadCloser)
// for bufReadCloser. It just passes the method call through to the wrapped
// io.ReadCloser.
func (b bufReadCloser) Close() error {
	return b.closefunc()
}

// newBufReadCloser wraps 'r' in a 'bufReadCloser', so that it can be peeked,
// etc.
func newBufReadCloser(r io.ReadCloser) io.ReadCloser {
	if r == nil {
		return nil
	}
	return bufReadCloser{
		Reader:    bufio.NewReaderSize(r, bodyPrefixLength),
		closefunc: r.Close,
	}
}

func bodyMsg(rc io.ReadCloser) string {
	if rc == nil {
		return "(empty)"
	}
	// bodyMsg is only called on {req,resp}.Body after it's been set with
	// newBufReadCloser, so this cast is safe
	bodyPrefix, err := rc.(bufReadCloser).Peek(bodyPrefixLength)
	bodyMsg := bytes.NewBuffer(bodyPrefix)
	if err != nil && !errors.Is(err, io.EOF) {
		bodyMsg.WriteString(fmt.Sprintf(" (error reading body: %v)", err))
	}
	return bodyMsg.String()
}

// loggingRoundTripper is an internal implementation of http.RoundTripper with
// which we wrap our kubernetes clients' own transport, so that we can log our
// kubernetes requests and responses.
type loggingRoundTripper struct {
	underlying      http.RoundTripper
	cancellations   int
	resetKubeClient func()
}

// RoundTrip implements the http.RoundTripper interface for loggingRoundTripper
func (t *loggingRoundTripper) RoundTrip(req *http.Request) (res *http.Response, retErr error) {
	requestID := uuid.New()
	start := time.Now()

	// Peek into req. body and log a prefix
	if req.Context() != nil && req.Context() != context.Background() {
		go func() {
			<-req.Context().Done()
			log.WithFields(log.Fields{
				"service":          "k8s.io/client-go",
				"method":           "Timeout",
				"start":            start.Format(time.StampMicro),
				"context-duration": time.Since(start).Seconds(),
				"pach-request-id":  requestID,
			}).Debug()
		}()
	}
	req.Body = newBufReadCloser(req.Body)
	log.WithFields(log.Fields{
		"service":         "k8s.io/client-go",
		"method":          "Request",
		"start":           start.Format(time.StampMicro),
		"delay":           time.Since(start).Seconds(),
		"http-method":     req.Method,
		"headers":         req.Header,
		"url":             req.URL.String(),
		"pach-request-id": requestID,
		"body":            bodyMsg(req.Body),
	}).Debug()

	// Log response
	defer func() {
		le := log.WithFields(log.Fields{
			"service":         "k8s.io/client-go",
			"method":          "Response",
			"start":           start.Format(time.StampMicro),
			"now":             time.Now().Format(time.StampMicro),
			"duration":        time.Since(start),
			"http-method":     req.Method,
			"url":             req.URL.String(),
			"pach-request-id": requestID,
			"err":             retErr,
		})
		if res != nil {
			// Peek into res. body and log a prefix
			res.Body = newBufReadCloser(res.Body)
			le = le.WithFields(log.Fields{
				"status":  res.Status,
				"headers": res.Header,
				"body":    bodyMsg(res.Body),
			})
		}
		le.Debug()
	}()
	return t.underlying.RoundTrip(req)
}

func (t *loggingRoundTripper) CancelRequest(req *http.Request) {
	t.cancellations++
	if t.cancellations >= cancellationsBeforeReset {
		t.resetKubeClient()
		t.cancellations = 0
	}
	c, ok := t.underlying.(kubeRequestCanceler)
	if !ok {
		log.Errorf("loggingRoundTripper: underlying RoundTripper %T does not implement CancelRequest", t.underlying)
		return
	}

	// Peek into req. body and log a prefix
	req.Body = newBufReadCloser(req.Body)
	defer func(start time.Time) {
		log.WithFields(log.Fields{
			"service":                    "k8s.io/client-go",
			"method":                     "Cancel",
			"start":                      start.Format(time.StampMicro),
			"duration":                   time.Since(start),
			"cancellations-before-reset": fmt.Sprintf("%d/%d", t.cancellations, cancellationsBeforeReset),
			"http-method":                req.Method,
			"url":                        req.URL.String(),
			"body":                       bodyMsg(req.Body),
		}).Debug()
	}(time.Now())

	// Print a stack trace, so we know where to look in the k8s client
	debug.PrintStack()

	// Cancel the underlying request
	c.CancelRequest(req)
}

// wrapWithLoggingTransport is k8s.io/client-go/transport.WrapperFunc that wraps
// a pre-existing http.RoundTripper in loggingRoundTripper. We pass this to our
// kubernetes client via rest.Config.WrapTransport to log our kubernetes RPCs
func wrapWithLoggingTransport(resetKubeClient func()) func(rt http.RoundTripper) http.RoundTripper {
	return func(rt http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{
			resetKubeClient: resetKubeClient,
			underlying:      rt,
		}
	}
}

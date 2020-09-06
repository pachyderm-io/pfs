package backoff

import (
	"context"
	"time"
)

// An Operation is executing by Retry() or RetryNotify().
// The operation will be retried using a backoff policy if it returns an error.
type Operation func() error

// Notify is a notify-on-error function. It receives an operation error and
// backoff delay if the operation failed (with an error).
//
// If the notify function returns an error itself, we stop retrying and return
// the error.
//
// NOTE that if the backoff policy stated to stop retrying,
// the notify function isn't called.
type Notify func(error, time.Duration) error

// Retry the operation o until it does not return error or BackOff stops.
// o is guaranteed to be run at least once.
// It is the caller's responsibility to reset b after Retry returns.
//
// Retry sleeps the goroutine for the duration returned by BackOff after a
// failed operation returns.
func Retry(o Operation, b BackOff) error { return RetryNotify(o, b, nil) }

// RetryNotify calls notify function with the error and wait duration
// for each failed attempt before sleep.
func RetryNotify(operation Operation, b BackOff, notify Notify) error {
	var err error
	var next time.Duration

	b.Reset()
	for {
		if err = operation(); err == nil {
			return nil
		}

		if next = b.NextBackOff(); next == Stop {
			return err
		}

		if notify != nil {
			if err := notify(err, next); err != nil {
				return err
			}
		}

		time.Sleep(next)
	}
}

// RetryUntilCancel is the same as RetryNotify, except that it will not retry if
// the given context is canceled.
func RetryUntilCancel(ctx context.Context, operation Operation, b BackOff, notify Notify) error {
	return RetryNotify(operation, b, func(err error, d time.Duration) error {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return notify(err, d)
		}
	})
}

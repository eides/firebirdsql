package firebirdsql

import (
	"context"
	"net"
	"time"
)

// watchConnectionContext covers handshake, authentication and database attach.
// It watches the raw net.Conn, whose deadline operations are concurrency-safe,
// without touching mutable cipher or protocol state. finish must run exactly
// once, before handing a successful connection to database/sql.
func watchConnectionContext(ctx context.Context, conn net.Conn) (finish func(error) error, err error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ctx.Done() == nil {
		return func(err error) error { return err }, nil
	}
	deadline, hasDeadline := ctx.Deadline()
	if hasDeadline {
		if err := conn.SetDeadline(deadline); err != nil {
			return nil, err
		}
	}
	stop := make(chan struct{})
	done := make(chan struct{})
	go func() {
		defer close(done)
		select {
		case <-ctx.Done():
			_ = conn.SetDeadline(time.Now())
		case <-stop:
		}
	}()
	return func(operationErr error) error {
		close(stop)
		<-done // A cancellation must never set a stale deadline after pool handoff.
		if err := ctx.Err(); err != nil {
			return err
		}
		// The socket timer can fire just before the context's timer is scheduled.
		if hasDeadline && !time.Now().Before(deadline) {
			return context.DeadlineExceeded
		}
		if operationErr != nil {
			return operationErr
		}
		return conn.SetDeadline(time.Time{})
	}, nil
}

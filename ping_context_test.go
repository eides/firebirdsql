package firebirdsql

import (
	"context"
	"database/sql/driver"
	"errors"
	"testing"
	"time"
)

// setPingWatcherHooks installs the Ping watcher test hooks for one test and
// removes them at cleanup. Tests that use it must not run in parallel.
func setPingWatcherHooks(t *testing.T, start func(ctx context.Context), exit func()) {
	t.Helper()
	testHookPingWatcherStart = start
	testHookPingWatcherExit = exit
	t.Cleanup(func() {
		testHookPingWatcherStart = nil
		testHookPingWatcherExit = nil
	})
}

// Regression: a cancellation that reaches the Ping watcher only after the
// round-trip must not leave a deadline on a connection Ping reported as good.
// The watcher is held until the context is done, which is what a starved
// scheduler does to it. Before the fix Ping returned nil without waiting for
// the watcher, and the late watcher could set a deadline in the past on a
// connection that database/sql had already put back in the pool: the next
// operation on it, even with a context without deadline, failed at once with
// "i/o timeout" (seen in the field as "begin: read tcp ...: i/o timeout").
//
// The check runs on the same driver connection, through Raw: going through
// the pool would let database/sql retry on a fresh connection whenever the
// stale deadline surfaces as ErrBadConn, and hide the bug. With both of its
// cases ready, the old watcher's select picks one at random, so each round
// shows the bug with probability 1/2; 16 rounds leave a false pass at 1 in
// 65536. With the fix no round can fail: Ping waits for the watcher, and a
// context done before Ping finishes makes it return ErrBadConn.
func TestPingCancelAfterRoundTripLeavesNoStaleDeadline(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_ping_stale_deadline_")

	exited := make(chan struct{}, 1)
	setPingWatcherHooks(t,
		func(ctx context.Context) { <-ctx.Done() },
		func() { exited <- struct{}{} },
	)

	for round := 0; round < 16; round++ {
		conn, err := db.Conn(context.Background())
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}

		var pingErr, nextErr error
		_ = conn.Raw(func(dc any) error {
			fc := dc.(*firebirdsqlConn)
			ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
			pingErr = fc.Ping(ctx)
			cancel()
			select {
			case <-exited:
			case <-time.After(5 * time.Second):
				nextErr = errors.New("the Ping watcher did not exit")
				return nextErr
			}
			if pingErr != nil {
				return pingErr // database/sql discards the connection
			}
			// The operation that failed in the field. Its context has no
			// deadline: only a stale one can fail it.
			tx, err := fc.BeginTx(context.Background(), driver.TxOptions{})
			if err != nil {
				nextErr = err
				return nextErr
			}
			return tx.Rollback()
		})
		_ = conn.Close()

		if pingErr != nil && !errors.Is(pingErr, driver.ErrBadConn) {
			t.Fatalf("round %d: Ping failed without ErrBadConn, so the connection would go back to the pool: %v", round, pingErr)
		}
		if nextErr != nil {
			t.Fatalf("round %d: Ping returned nil, then the next operation on the same connection failed: %v", round, nextErr)
		}
	}
}

// A cancellation that arrives while the ping is in flight leaves the wire
// position unknown: Ping must report driver.ErrBadConn, so database/sql drops
// the connection instead of returning it to the pool. The watcher cancels the
// context itself, before Ping can finish.
func TestPingCancelledDuringRoundTripIsBadConn(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_ping_cancel_badconn_")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	setPingWatcherHooks(t, func(context.Context) { cancel() }, nil)

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	err = conn.Raw(func(dc any) error {
		return dc.(*firebirdsqlConn).Ping(ctx)
	})
	if !errors.Is(err, driver.ErrBadConn) || !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping cancelled mid round-trip: err = %v, want context.Canceled and driver.ErrBadConn", err)
	}

	// The pool replaces the discarded connection.
	var one int
	if err := db.QueryRowContext(context.Background(), "select 1 from rdb$database").Scan(&one); err != nil {
		t.Fatalf("operation after the discarded connection: %v", err)
	}
}

// A ping whose context has no deadline must fail, not hang, when the read
// times out on a deadline it did not set. Before the fix the timeout path
// waited on ctx.Done(), which is nil for context.Background(): Ping blocked
// forever on a connection left with a stale deadline.
func TestPingWithoutDeadlineDoesNotHangOnStaleDeadline(t *testing.T) {
	db, _, _ := createTestDatabaseWithDDL(t, "test_ping_nil_done_")

	conn, err := db.Conn(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan error, 1)
	go func() {
		result <- conn.Raw(func(dc any) error {
			fc := dc.(*firebirdsqlConn)
			_ = fc.wp.conn.SetDeadline(time.Now()) // a deadline nobody clears
			return fc.Ping(context.Background())
		})
	}()

	select {
	case err := <-result:
		_ = conn.Close()
		if !errors.Is(err, driver.ErrBadConn) {
			t.Fatalf("Ping on a stale deadline: err = %v, want driver.ErrBadConn", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Ping with context.Background() hung on a stale deadline")
	}
}

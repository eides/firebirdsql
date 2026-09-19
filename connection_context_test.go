package firebirdsql

import (
	"context"
	"database/sql/driver"
	"encoding/binary"
	"errors"
	"io"
	"net"
	"testing"
	"time"
)

func TestForkRegisteredDriverContext(t *testing.T) {
	var d driver.Driver = &firebirdsqlDriver{}
	dc, ok := d.(driver.DriverContext)
	if !ok {
		t.Fatal("registered driver must expose OpenConnector")
	}
	connector, err := dc.OpenConnector("u:p@127.0.0.1:1/test.fdb")
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	conn, err := connector.Connect(ctx)
	if conn != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("conn=%v err=%v", conn, err)
	}
}

func TestForkConnectStages(t *testing.T) {
	for _, stage := range []string{"handshake", "auth", "attach", "reject"} {
		t.Run(stage, func(t *testing.T) {
			ln, err := net.Listen("tcp", "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			defer ln.Close()
			done := make(chan error, 1)
			go func() {
				p, e := ln.Accept()
				if e != nil {
					done <- e
					return
				}
				defer p.Close()
				_ = p.SetDeadline(time.Now().Add(2 * time.Second))
				var words []int32
				switch stage {
				case "auth":
					words = []int32{op_accept_data, 13, 1, 2, 0} // stops before auth plugin length
				case "attach":
					words = []int32{op_accept, 13, 1, 2}
				case "reject":
					words = []int32{op_reject}
				}
				for _, w := range words {
					if e = binary.Write(p, binary.BigEndian, w); e != nil {
						done <- e
						return
					}
				}
				_, e = io.Copy(io.Discard, p)
				done <- e // EOF proves failed connect closed the socket.
			}()
			dsn, err := parseDSN("u:p@" + ln.Addr().String() + "/test.fdb?wire_crypt=false")
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Millisecond)
			defer cancel()
			attached := false
			start := time.Now()
			conn, err := openFirebirdsqlConnContext(ctx, dsn, func(wp *wireProtocol) error {
				attached = true
				return wp.opAttach(dsn.dbName, dsn.user, dsn.passwd, "")
			})
			elapsed := time.Since(start)
			if conn != nil || err == nil {
				t.Fatalf("conn=%v err=%v", conn, err)
			}
			if stage != "reject" && !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("expected deadline: %v", err)
			}
			if stage == "reject" && errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("protocol rejection masked: %v", err)
			}
			if stage == "attach" && !attached {
				t.Fatal("attach stage never reached")
			}
			if elapsed > 500*time.Millisecond {
				t.Errorf("late return: %s", elapsed)
			}
			select {
			case e := <-done:
				if e != nil {
					t.Errorf("server did not observe clean socket close: %v", e)
				}
			case <-time.After(time.Second):
				t.Error("failed connect leaked socket")
			}
		})
	}
}

func TestForkWatcherStopsBeforeReuse(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	defer cancel()
	finish, err := watchConnectionContext(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	if err = finish(nil); err != nil {
		t.Fatal(err)
	}
	<-ctx.Done()
	go func() { _, _ = b.Write([]byte{42}) }()
	read := make(chan error, 1)
	go func() {
		var buf [1]byte
		_, err := io.ReadFull(a, buf[:])
		if err == nil && buf[0] != 42 {
			err = errors.New("bad value")
		}
		read <- err
	}()
	select {
	case err := <-read:
		if err != nil {
			t.Fatalf("stale connection deadline after handoff: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("read stalled")
	}
}

func TestForkManualCancelRead(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	finish, err := watchConnectionContext(ctx, a)
	if err != nil {
		t.Fatal(err)
	}
	cancel()
	var buf [1]byte
	_, err = a.Read(buf[:])
	if err = finish(err); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected cancellation: %v", err)
	}
}

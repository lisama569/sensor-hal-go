package bus

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func newTestBus() *MemBus {
	now := time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)
	clk := func() time.Time { return now }
	return NewMemBus(map[uint8]uint16{0x00: 0xA1, 0x01: 0x0201}, clk)
}

func TestReadWriteAndLog(t *testing.T) {
	b := newTestBus()
	ctx := context.Background()

	if err := b.Write(ctx, 0x10, 0x1234); err != nil {
		t.Fatalf("write: %v", err)
	}
	v, err := b.Read(ctx, 0x10)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if v != 0x1234 {
		t.Fatalf("want 0x1234, got 0x%04X", v)
	}

	log := b.Log()
	if len(log) != 2 {
		t.Fatalf("want 2 log entries, got %d", len(log))
	}
	if log[0].Kind != OpWrite || log[0].Values[0] != 0x1234 {
		t.Fatalf("unexpected log[0]: %+v", log[0])
	}
	if log[1].Kind != OpRead {
		t.Fatalf("unexpected log[1]: %+v", log[1])
	}
	t.Logf("激励日志: %#v", log)
}

func TestBurstAtomicSnapshot(t *testing.T) {
	b := newTestBus()
	vals, err := b.ReadBurst(context.Background(), 0x00, 2)
	if err != nil {
		t.Fatal(err)
	}
	if vals[0] != 0xA1 || vals[1] != 0x0201 {
		t.Fatalf("burst mismatch: %#v", vals)
	}
}

func TestFaultTimeout(t *testing.T) {
	b := newTestBus()
	b.AddFault(FaultRule{Kind: OpRead, Addr: 0x00, Fault: FaultTimeout})
	_, err := b.Read(context.Background(), 0x00)
	if !errors.Is(err, ErrTimeout) {
		t.Fatalf("want ErrTimeout, got %v", err)
	}
	if got := b.Log()[0].Fault; got != FaultTimeout {
		t.Fatalf("log fault = %v", got)
	}
}

func TestFaultTimeoutRespectsContext(t *testing.T) {
	b := newTestBus()
	b.AddFault(FaultRule{Kind: OpRead, Addr: 0x00, Fault: FaultTimeout, Delay: time.Second})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err := b.Read(ctx, 0x00)
	if time.Since(start) > 200*time.Millisecond {
		t.Fatalf("timeout fault did not respect ctx")
	}
	if err == nil {
		t.Fatal("want error")
	}
}

func TestFaultRejectDoesNotMutate(t *testing.T) {
	b := newTestBus()
	b.AddFault(FaultRule{Kind: OpWrite, Addr: 0x10, Fault: FaultReject})
	if err := b.Write(context.Background(), 0x10, 0xBEEF); !errors.Is(err, ErrRejected) {
		t.Fatalf("want ErrRejected, got %v", err)
	}
	if b.Snapshot()[0x10] != 0 {
		t.Fatalf("rejected write must not mutate registers")
	}
}

func TestFaultCorruptReadAndTimes(t *testing.T) {
	b := newTestBus()
	// 仅篡改 1 次：第一次读得到取反值，第二次恢复。
	b.AddFault(FaultRule{Kind: OpRead, Addr: 0x00, Times: 1, Fault: FaultCorrupt})
	v, err := b.Read(context.Background(), 0x00)
	if err != nil || v != ^uint16(0xA1) {
		t.Fatalf("first read want corrupt, got 0x%04X err=%v", v, err)
	}
	v, err = b.Read(context.Background(), 0x00)
	if err != nil || v != 0xA1 {
		t.Fatalf("second read want clean, got 0x%04X err=%v", v, err)
	}
}

func TestFaultCorruptWrite(t *testing.T) {
	b := newTestBus()
	b.AddFault(FaultRule{Kind: OpWrite, Addr: 0x10, Fault: FaultCorrupt})
	if err := b.Write(context.Background(), 0x10, 0x1234); err != nil {
		t.Fatal(err)
	}
	if got := b.Snapshot()[0x10]; got != ^uint16(0x1234) {
		t.Fatalf("corrupt write: register = 0x%04X", got)
	}
	// 主机回读必然与其发出值不符。
	got, _ := b.Read(context.Background(), 0x10)
	if got == 0x1234 {
		t.Fatal("corrupt write unexpectedly read back equal")
	}
}

func TestSnapshotIsCopy(t *testing.T) {
	b := newTestBus()
	s := b.Snapshot()
	s[0x00] = 0xFFFF
	if b.Snapshot()[0x00] != 0xA1 {
		t.Fatal("snapshot must be a copy")
	}
}

func TestLogStringers(t *testing.T) {
	if s := OpBurst.String(); s != "BURST" {
		t.Fatal(s)
	}
	if !strings.Contains(FaultTimeout.String(), "TIMEOUT") {
		t.Fatal("fault string")
	}
}

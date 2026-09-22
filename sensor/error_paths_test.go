package sensor

import (
	"context"
	"testing"

	"github.com/lisama569/sensor-hal-go/bus"
)

func mustKind(t *testing.T, err error, want Kind) *Error {
	t.Helper()
	se, ok := AsError(err)
	if !ok {
		t.Fatalf("want *sensor.Error, got %T: %v", err, err)
	}
	if se.Kind != want {
		t.Fatalf("want kind %s, got %s (%v)", want, se.Kind, err)
	}
	t.Logf("错误分类符合预期 kind=%s attempts=%d detail=%q", se.Kind, se.Attempts, se.Detail)
	return se
}

func TestProbeTimeoutThenSuccess(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	// 前 1 次读 CHIPID 超时，第 2 次成功：驱动重试后整体成功。
	r.b.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: AddrChipID, Times: 1, Fault: bus.FaultTimeout})
	c, err := r.drv.Probe(context.Background())
	if err != nil {
		t.Fatalf("probe should recover after retry: %v", err)
	}
	if !c.OK {
		t.Fatal("compat ok")
	}
}

func TestProbeTimeoutExhausted(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	r.b.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: AddrChipID, Fault: bus.FaultTimeout})
	_, err := r.drv.Probe(context.Background())
	se := mustKind(t, err, KindTimeout)
	if se.Attempts != r.drv.opts.ReadRetry+1 {
		t.Fatalf("want %d attempts, got %d", r.drv.opts.ReadRetry+1, se.Attempts)
	}
	if se.Retryable {
		t.Fatal("terminal error must not be retryable")
	}
	// 每次尝试都应在总线日志留痕，共 retry+1 次。
	var timeoutReads int
	for _, e := range r.b.Log() {
		if e.Addr == AddrChipID && e.Fault == bus.FaultTimeout {
			timeoutReads++
		}
	}
	if timeoutReads != r.drv.opts.ReadRetry+1 {
		t.Fatalf("bus log want %d timeout reads, got %d", r.drv.opts.ReadRetry+1, timeoutReads)
	}
}

func TestProbeRejectedNotRetried(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	r.b.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: AddrChipID, Fault: bus.FaultReject})
	_, err := r.drv.Probe(context.Background())
	se := mustKind(t, err, KindRejected)
	if se.Attempts != 1 {
		t.Fatalf("reject must not be retried, attempts=%d", se.Attempts)
	}
}

func TestInitReadbackMismatch(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	// ODR 写线上被篡改 => 设备寄存器得到取反值 => 回读与期望不符。
	r.b.AddFault(bus.FaultRule{Kind: bus.OpWrite, Addr: AddrODR, Times: 1, Fault: bus.FaultCorrupt})
	err := r.drv.Init(context.Background(), Config{ODR: ODR100, FSR: FSR4})
	mustKind(t, err, KindReadbackMismatch)
	if r.drv.State() != StateOff {
		t.Fatalf("failed init must not move device to ACTIVE, got %s", r.drv.State())
	}
}

func TestInitRejectedByDevice(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	// 上电写被拒绝（模拟硬件异常）。
	r.b.AddFault(bus.FaultRule{Kind: bus.OpWrite, Addr: AddrCtrl, Times: 1, Fault: bus.FaultReject})
	err := r.drv.Init(context.Background(), Config{ODR: ODR100, FSR: FSR4})
	mustKind(t, err, KindRejected)
}

func TestBadConfigTouchesNoBus(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	err := r.drv.Init(context.Background(), Config{ODR: Hz(7), FSR: FSR4})
	mustKind(t, err, KindBadConfig)
	if len(r.b.Log()) != 0 {
		t.Fatalf("bad config must not touch the bus, log=%d entries", len(r.b.Log()))
	}
}

func TestIncompatibleReasons(t *testing.T) {
	cases := []struct {
		name   string
		mut    func(r *rig)
		reason CompatReason
	}{
		{"unknown chip", func(r *rig) {
			r.b.Direct(func(v bus.RegisterView) { v.Set(AddrChipID, 0xBAD) })
		}, CompatUnknownChip},
		{"newer major", func(r *rig) {
			r.b.Direct(func(v bus.RegisterView) { v.Set(AddrVersion, EncodeVersion(RegVersion{3, 0})) })
		}, CompatMajorMismatch},
		{"device too old", func(r *rig) {
			r.b.Direct(func(v bus.RegisterView) { v.Set(AddrVersion, EncodeVersion(RegVersion{2, 0})) })
		}, CompatTooOld},
		{"unreadable", func(r *rig) {
			r.b.Direct(func(v bus.RegisterView) { v.Set(AddrVersion, 0xFFFF) })
		}, CompatUnreadable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := newRig(t)
			defer r.dumpBus()
			tc.mut(r)
			c, err := r.drv.Probe(context.Background())
			mustKind(t, err, KindIncompatible)
			if c.Reason != tc.reason {
				t.Fatalf("want reason %s, got %s", tc.reason, c.Reason)
			}
			// 不兼容时 Init 同样失败且不会进入 ACTIVE。
			if err := r.drv.Init(context.Background(), Config{ODR: ODR100, FSR: FSR4}); err == nil {
				t.Fatal("init must fail on incompatible device")
			}
			if r.drv.State() != StateOff {
				t.Fatalf("state=%s", r.drv.State())
			}
		})
	}
}

func TestNewerMinorAccepted(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	r.b.Direct(func(v bus.RegisterView) { v.Set(AddrVersion, EncodeVersion(RegVersion{2, 9})) })
	if err := r.drv.Init(context.Background(), Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatalf("newer minor must be accepted: %v", err)
	}
}

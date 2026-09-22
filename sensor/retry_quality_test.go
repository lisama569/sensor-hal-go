package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
)

func TestWriteNotRetriedNoDoubleSideEffect(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	// Ctrl 写仅前 1 次被拒绝。
	r.b.AddFault(bus.FaultRule{Kind: bus.OpWrite, Addr: AddrCtrl, Times: 1, Fault: bus.FaultReject})
	err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4})
	mustKind(t, err, KindRejected)

	// 写绝不能重试：日志中对 Ctrl 只能有 1 次写事务。
	writes := 0
	for _, e := range r.b.Log() {
		if e.Kind == bus.OpWrite && e.Addr == AddrCtrl {
			writes++
		}
	}
	if writes != 1 {
		t.Fatalf("write must not be retried; ctrl writes = %d", writes)
	}
}

func TestTransientTimeoutRecoversExactly(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	// CHIPID 前两次超时、第三次成功；重试上限 2 => 恰好成功。
	r.b.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: AddrChipID, Times: 2, Fault: bus.FaultTimeout})
	if _, err := r.drv.Probe(ctx); err != nil {
		t.Fatalf("probe should recover: %v", err)
	}
	var reads int
	for _, e := range r.b.Log() {
		if e.Kind == bus.OpRead && e.Addr == AddrChipID {
			reads++
		}
	}
	if reads != 3 {
		t.Fatalf("want exactly 3 chipid reads (2 fail + 1 ok), got %d", reads)
	}
}

func TestNeedsRecalQuality(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	r.sim.SetStatus(r.b, StatusRCAL)
	r.clk.advance(11 * time.Millisecond)
	s, err := r.drv.Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Quality != QNeedsRecal {
		t.Fatalf("want QNeedsRecal, got %s", s.Quality)
	}
	t.Logf("质量标记符合预期: %+v", s)
}

func TestSampleContextCanceled(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx, cancel := context.WithCancel(context.Background())
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	// 不推进时钟，采样在轮询中；外部取消后快速返回 CANCELED。
	cancel()
	start := time.Now()
	_, err := r.drv.Sample(ctx)
	mustKind(t, err, KindCanceled)
	if time.Since(start) > time.Second {
		t.Fatal("canceled sample returned too late")
	}
}

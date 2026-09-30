package observe_test

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
	"github.com/lisama569/sensor-hal-go/hal"
	"github.com/lisama569/sensor-hal-go/observe"
	"github.com/lisama569/sensor-hal-go/sensor"
)

type testLogger struct{ t *testing.T }

func (l testLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }

func newRealStack(t *testing.T, sampleCap, failCap int) (*observe.Observer, *hal.HAL, *bus.MemBus) {
	t.Helper()
	regs := map[uint8]uint16{
		sensor.AddrChipID:  sensor.ChipIDA,
		sensor.AddrVersion: sensor.EncodeVersion(sensor.RegVersion{Major: 2, Minor: 1}),
	}
	mb := bus.NewMemBus(regs, nil)
	sim := sensor.NewSimDevice(nil)
	mb.SetModel(sim)
	drv := sensor.New(mb, sensor.RegVersion{Major: 2, Minor: 1}, testLogger{t}, sensor.DefaultOptions())
	h := hal.New(drv)
	if err := h.Open(context.Background(), sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	o := observe.New(h, observe.Options{
		SampleCapacity:  sampleCap,
		FailureCapacity: failCap,
		RetryInterval:   time.Millisecond,
	})
	return o, h, mb
}

// TestObserverOverRealHAL 端到端：真实 HAL + 模拟设备上连续采集，
// 近期帧完整、序号单调；挂起期间产生可查询的 SUSPENDED 失败记录，
// 恢复后继续采集；停止不产生 CANCELED 失败。
func TestObserverOverRealHAL(t *testing.T) {
	o, h, _ := newRealStack(t, 8, 8)
	ctx, stop := o.Start()
	defer func() { stop(); o.Wait() }()

	waitSamples := func(want int64) {
		t.Helper()
		deadline := time.Now().Add(2 * time.Second)
		for o.Stats().SamplesTotal < want {
			if time.Now().After(deadline) {
				t.Fatalf("samples total = %d, want >= %d", o.Stats().SamplesTotal, want)
			}
			time.Sleep(2 * time.Millisecond)
		}
	}
	waitSamples(10)

	batch := o.Recent(5)
	if batch.Status != observe.StatusOK || len(batch.Frames) != 5 {
		t.Fatalf("recent = %+v", batch)
	}
	for i := 1; i < len(batch.Frames); i++ {
		if batch.Frames[i].Seq <= batch.Frames[i-1].Seq {
			t.Fatalf("frames not ordered: %d then %d", batch.Frames[i-1].Seq, batch.Frames[i].Seq)
		}
		cur, prev := batch.Frames[i].Sample, batch.Frames[i-1].Sample
		if cur.Y != cur.X*2 || prev.Y != prev.X*2 {
			t.Fatal("torn frame in recent batch")
		}
	}

	// 挂起：采集循环遇到 SUSPENDED 失败并记录，然后重试不空转。
	if err := h.Suspend(context.Background()); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		found := false
		for _, f := range o.Failures() {
			if f.Code == hal.CodeSuspended {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("suspension not recorded: %+v", o.Failures())
		}
		time.Sleep(2 * time.Millisecond)
	}

	// 恢复后采集继续，序号在挂起前后保持单调。
	before := o.Stats().LatestSeq
	if err := h.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitSamples(before + 3)
	after := o.Since(before, 10)
	if after.Status != observe.StatusOK || len(after.Frames) == 0 {
		t.Fatalf("post-resume batch: %+v", after)
	}
	for _, f := range after.Frames {
		if f.Seq <= before {
			t.Fatalf("post-resume seq not ahead: %d <= %d", f.Seq, before)
		}
	}

	// 主动停止：失败缓存里不得出现 CANCELED。
	stop()
	o.Wait()
	for _, f := range o.Failures() {
		if f.Code == hal.CodeCanceled {
			t.Fatalf("caller stop recorded as failure: %+v", f)
		}
	}
	if err := ctx.Err(); err == nil {
		t.Fatal("observer ctx should be canceled after stop")
	}
}

// TestObserverCapacityStableOverLongRun 验证长期运行占用平稳：
// 写入量远超容量后，条数恒定停在容量上。
func TestObserverCapacityStableOverLongRun(t *testing.T) {
	const cap = 4
	o, _, _ := newRealStack(t, cap, cap)
	_, stop := o.Start()
	defer func() { stop(); o.Wait() }()

	deadline := time.Now().Add(3 * time.Second)
	for o.Stats().SamplesTotal < int64(cap*10) {
		if time.Now().After(deadline) {
			t.Fatalf("only collected %d", o.Stats().SamplesTotal)
		}
		time.Sleep(2 * time.Millisecond)
	}
	st := o.Stats()
	if st.SampleCount != cap {
		t.Fatalf("count = %d, want pinned at %d (total=%d)", st.SampleCount, cap, st.SamplesTotal)
	}
	batch := o.Recent(0)
	if len(batch.Frames) != cap {
		t.Fatalf("recent len = %d", len(batch.Frames))
	}
	// 游标指向最早的帧（早已被挤出）=> EVICTED，必须与 EMPTY 可区分。
	stale := o.Since(1, 10)
	if stale.Status != observe.StatusEvicted {
		t.Fatalf("want EVICTED, got %s", stale.Status)
	}
}

// TestObserverStopsCleanly 补充：停止后 ctx 处于已取消状态。
func TestObserverStopsCleanly(t *testing.T) {
	o, _, _ := newRealStack(t, 4, 4)
	ctx, stop := o.Start()
	time.Sleep(10 * time.Millisecond)
	stop()
	o.Wait()
	if err := ctx.Err(); err == nil {
		t.Fatal("observer ctx should be canceled after stop")
	}
}

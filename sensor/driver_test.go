package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
)

// tLogger 把驱动/总线激励与状态迁移打印到单测日志。
type tLogger struct{ t *testing.T }

func (l tLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }

type rig struct {
	t   *testing.T
	b   *bus.MemBus
	sim *SimDevice
	drv *Driver
	clk *fakeClock
}

type fakeClock struct{ t time.Time }

func (c *fakeClock) now() time.Time          { return c.t }
func (c *fakeClock) advance(d time.Duration) { c.t = c.t.Add(d) }

func newRig(t *testing.T) *rig {
	t.Helper()
	fc := &fakeClock{t: time.Date(2026, 9, 23, 10, 0, 0, 0, time.UTC)}
	regs := map[uint8]uint16{
		AddrChipID:  ChipIDA,
		AddrVersion: EncodeVersion(RegVersion{Major: 2, Minor: 1}),
		AddrCtrl:    0,
	}
	mb := bus.NewMemBus(regs, fc.now)
	sim := NewSimDevice(fc.now)
	mb.SetModel(sim)
	opts := DefaultOptions()
	opts.Clock = fc.now
	drv := New(mb, RegVersion{Major: 2, Minor: 1}, tLogger{t}, opts)
	return &rig{t: t, b: mb, sim: sim, drv: drv, clk: fc}
}

// dumpBus 打印全部总线读写激励。
func (r *rig) dumpBus() {
	for _, e := range r.b.Log() {
		r.t.Logf("BUS #%d %s addr=0x%02X vals=%#x fault=%s",
			e.Seq, e.Kind, e.Addr, e.Values, e.Fault)
	}
}

func TestProbeInitSampleHappyPath(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()

	c, err := r.drv.Probe(ctx)
	if err != nil {
		t.Fatalf("probe: %v", err)
	}
	if !c.OK {
		t.Fatalf("compat not ok: %s", c.Message)
	}
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatalf("init: %v", err)
	}
	if r.drv.State() != StateActive {
		t.Fatalf("state = %s", r.drv.State())
	}

	// 推进墙钟超过一个 ODR 周期（100Hz => 10ms），触发转换。
	r.clk.advance(11 * time.Millisecond)
	s, err := r.drv.Sample(ctx)
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	if s.Quality != QOK || s.ODR != ODR100 || s.FSR != FSR4 {
		t.Fatalf("unexpected sample: %+v", s)
	}
	if s.X != 1 || s.Y != 2 || s.Z != 997 {
		t.Fatalf("unexpected triad: x=%d y=%d z=%d", s.X, s.Y, s.Z)
	}
	if s.T.IsZero() {
		t.Fatal("sample timestamp must be set")
	}
	t.Logf("采样成功: %+v", s)
}

func TestSampleTimeoutWhenNeverReady(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	// 不推进时钟 => 转换永不就绪 => 采样在 SampleTimeout 后超时。
	// 用真实截止时间控制等待上限（寄存器读取本身是瞬时的）。
	// 外层上限必须远大于内部 SampleTimeout（200ms），否则高负载调度饥饿时
	// 外层截止先触发会把结论翻成 CANCELED，失败归因随调度抖动。
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := r.drv.Sample(ctx)
	se, ok := AsError(err)
	if !ok || se.Kind != KindTimeout {
		t.Fatalf("want KindTimeout, got %v", err)
	}
	t.Logf("未就绪超时符合预期: %v", err)
}

func TestOverrunQuality(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	r.clk.advance(11 * time.Millisecond) // 第 1 帧就绪，未取走
	r.clk.advance(11 * time.Millisecond) // 第 2 帧覆盖第 1 帧
	s, err := r.drv.Sample(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if s.Quality != QOverrun {
		t.Fatalf("want QOverrun, got %s", s.Quality)
	}
	if s.X != 2 {
		t.Fatalf("overrun sample must be newest frame, got seq=%d", s.X)
	}
}

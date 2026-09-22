package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
)

func sampleOnce(t *testing.T, r *rig) Sample {
	t.Helper()
	r.clk.advance(11 * time.Millisecond)
	s, err := r.drv.Sample(context.Background())
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	return s
}

func TestSuspendBlocksAndResumeRestores(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	s1 := sampleOnce(t, r)

	if err := r.drv.Suspend(ctx); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if r.drv.State() != StateSuspended {
		t.Fatalf("state = %s", r.drv.State())
	}

	// 挂起期间采样必须立即失败，不发起总线事务，且不能返回挂起前的 s1。
	busOpsBefore := len(r.b.Log())
	_, err := r.drv.Sample(ctx)
	mustKind(t, err, KindSuspended)
	if len(r.b.Log()) != busOpsBefore {
		t.Fatal("suspended sample must not touch bus")
	}
	// 挂起期间 Init / Suspend / Probe 同样立即拒绝。
	mustKind(t, r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}), KindSuspended)
	mustKind(t, r.drv.Suspend(ctx), KindSuspended)

	// 恢复后配置仍在，采样得到的是新帧而不是陈旧的 s1。
	if err := r.drv.Resume(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if r.drv.State() != StateActive {
		t.Fatalf("state = %s", r.drv.State())
	}
	s2 := sampleOnce(t, r)
	if s2.T.Equal(s1.T) || s2.X == s1.X {
		t.Fatalf("resumed sample is stale: s1=%+v s2=%+v", s1, s2)
	}
}

func TestResumeAfterPowerLossReinitializes(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	if err := r.drv.Suspend(ctx); err != nil {
		t.Fatal(err)
	}

	// 模拟挂起期间突发掉电：配置寄存器复位、签名清零。
	r.sim.SimulatePowerLoss(r.b)
	snap := r.b.Snapshot()
	if snap[AddrConfSig] != 0 || snap[AddrCtrl] != 0 {
		t.Fatalf("power loss did not reset registers: %#x %#x", snap[AddrConfSig], snap[AddrCtrl])
	}

	// Resume 必须检测到签名丢失并重新初始化，而不是沿用驱动缓存状态。
	logBefore := len(r.b.Log())
	if err := r.drv.Resume(ctx); err != nil {
		t.Fatalf("resume after power loss: %v", err)
	}
	reconfigured := false
	for _, e := range r.b.Log()[logBefore:] {
		if e.Kind == bus.OpWrite && e.Addr == AddrConfSig {
			reconfigured = true
		}
	}
	if !reconfigured {
		t.Fatal("resume did not re-initialize after config loss")
	}
	if r.drv.State() != StateActive {
		t.Fatalf("state = %s", r.drv.State())
	}
	// 重新初始化后可以立即采样。
	_ = sampleOnce(t, r)
}

func TestPowerOffThenSampleRequiresInit(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	_ = sampleOnce(t, r)
	if err := r.drv.PowerOff(ctx); err != nil {
		t.Fatalf("poweroff: %v", err)
	}
	_, err := r.drv.Sample(ctx)
	mustKind(t, err, KindPoweredOff)

	// 掉电期间写配置寄存器会被设备拒绝（模型门控）；必须重新 Init。
	if err := r.drv.Init(ctx, Config{ODR: ODR200, FSR: FSR8}); err != nil {
		t.Fatalf("re-init: %v", err)
	}
	s := sampleOnce(t, r)
	if s.ODR != ODR200 || s.FSR != FSR8 {
		t.Fatalf("re-init config not applied: %+v", s)
	}
}

func TestIllegalTransitions(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()

	// OFF 状态：Resume 非法；Suspend 报掉电。
	mustKind(t, r.drv.Resume(ctx), KindIO)
	mustKind(t, r.drv.Suspend(ctx), KindPoweredOff)

	// PowerOff 幂等。
	if err := r.drv.PowerOff(ctx); err != nil {
		t.Fatalf("poweroff in OFF must be idempotent: %v", err)
	}

	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	// ACTIVE 状态 Resume 非法。
	mustKind(t, r.drv.Resume(ctx), KindIO)
	if err := r.drv.Suspend(ctx); err != nil {
		t.Fatal(err)
	}
	// 恢复后 Suspend 可再次执行（往返）。
	if err := r.drv.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.drv.Suspend(ctx); err != nil {
		t.Fatal(err)
	}
	if err := r.drv.Resume(ctx); err != nil {
		t.Fatal(err)
	}
	if r.drv.State() != StateActive {
		t.Fatalf("state = %s", r.drv.State())
	}
}

func TestSuspendResumeBusErrorLeavesStateUnchanged(t *testing.T) {
	r := newRig(t)
	defer r.dumpBus()
	ctx := context.Background()
	if err := r.drv.Init(ctx, Config{ODR: ODR100, FSR: FSR4}); err != nil {
		t.Fatal(err)
	}
	// 挂起写超时：状态必须停留在 ACTIVE，不允许虚假迁移。
	r.b.AddFault(bus.FaultRule{Kind: bus.OpWrite, Addr: AddrCtrl, Times: 1, Fault: bus.FaultTimeout})
	err := r.drv.Suspend(ctx)
	mustKind(t, err, KindTimeout)
	if r.drv.State() != StateActive {
		t.Fatalf("failed suspend must keep ACTIVE, got %s", r.drv.State())
	}
	r.b.ClearFaults()
	if err := r.drv.Suspend(ctx); err != nil {
		t.Fatalf("retry suspend after fault cleared: %v", err)
	}
}

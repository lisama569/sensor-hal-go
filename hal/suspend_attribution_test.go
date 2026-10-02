// 挂起/恢复电源切换压测下的失败归因测试。
//
// 关注点：设备侧挂起打断在途采集，结论必须稳定归到“设备挂起（SUSPENDED）”
// 这一类，绝不能随调度时机翻成“调用方主动结束（CANCELED）”；反复执行
// 同样的挂起/恢复操作，Observer 失败环里的归因与比例不得漂移。
package hal

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
	"github.com/lisama569/sensor-hal-go/sensor"
)

// blockStatusRead 让状态寄存器读在总线内等待 delay 后超时：
// 采样一旦发起即确定处于在途等待，挂起必然走“打断在途采集”路径。
func blockStatusRead(mb *bus.MemBus, delay time.Duration) {
	mb.AddFault(bus.FaultRule{
		Kind:  bus.OpRead,
		Addr:  sensor.AddrStatus,
		Fault: bus.FaultTimeout,
		Delay: delay,
	})
}

// TestReadAbortedBySuspendIsAlwaysSuspended 直接路径压测：
// 不管挂起落在采样过程的哪个时刻（在途打断 / 挂起后才进入驱动），
// 也不管在挂起返回前后调用方是否取消自己的 ctx，设备侧中断的结论
// 都必须是 SUSPENDED；只有调用方自己先取消才允许是 CANCELED。
func TestReadAbortedBySuspendIsAlwaysSuspended(t *testing.T) {
	const rounds = 20
	for i := 0; i < rounds; i++ {
		h, mb, _ := newHAL(t)
		ctx := context.Background()
		if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
			t.Fatal(err)
		}
		blockStatusRead(mb, 40*time.Millisecond)

		rctx, cancel := context.WithCancel(ctx)
		readErr := make(chan error, 1)
		go func() { _, err := h.Read(rctx); readErr <- err }()
		time.Sleep(3 * time.Millisecond) // 采样确定已在途

		if err := h.Suspend(ctx); err != nil {
			t.Fatalf("round %d: suspend: %v", i, err)
		}

		// 交替在 read 返回前/后取消调用方 ctx，覆盖两种调度先后，
		// 结论都不得翻成 CANCELED。
		if i%2 == 0 {
			cancel()
		}
		select {
		case err := <-readErr:
			mustCode(t, err, CodeSuspended)
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: aborted read never returned", i)
		}
		cancel()

		// 已挂起后再发起读：同样是设备侧 SUSPENDED，不触总线、不给陈旧帧。
		done := make(chan error, 1)
		go func() { _, err := h.Read(ctx); done <- err }()
		select {
		case err := <-done:
			mustCode(t, err, CodeSuspended)
		case <-time.After(2 * time.Second):
			t.Fatalf("round %d: read while suspended blocked", i)
		}

		mb.ClearFaults()
		if err := h.Resume(ctx); err != nil {
			t.Fatalf("round %d: resume: %v", i, err)
		}
	}
	t.Logf("挂起打断在途采样 ×%d 轮结论全部为 SUSPENDED（含两种取消调度先后）", rounds)
}

// TestCallerCancelStaysCanceledWhileSuspendPending 区分性对照：
// 调用方先主动取消自己 ctx 的采样是 CANCELED，即便此后设备进入挂起；
// 这是调用方行为，与设备侧打断必须可分。
func TestCallerCancelStaysCanceledWhileSuspendPending(t *testing.T) {
	h, mb, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	blockStatusRead(mb, 40*time.Millisecond)

	rctx, cancel := context.WithCancel(ctx)
	readErr := make(chan error, 1)
	go func() { _, err := h.Read(rctx); readErr <- err }()
	time.Sleep(3 * time.Millisecond)

	cancel() // 调用方先主动结束
	select {
	case err := <-readErr:
		mustCode(t, err, CodeCanceled)
	case <-time.After(2 * time.Second):
		t.Fatal("caller-canceled read never returned")
	}

	// 随后挂起设备：设备状态迁移不受影响，下一次读为 SUSPENDED。
	if err := h.Suspend(ctx); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	_, err := h.Read(ctx)
	mustCode(t, err, CodeSuspended)
}

// TestObserverSuspendResumeAttributionStable 是核心压测：
// 反复挂起/恢复多轮，Observer 失败环中与挂起相关的记录必须逐条都是
// SUSPENDED，不得混入任何 CANCELED；挂起期间无新帧、无陈旧/乱序帧；
// 恢复后采样序号严格接续；失败记录与最近采样始终能一起查到。
func TestObserverSuspendResumeAttributionStable(t *testing.T) {
	h, o := startObserver(t, 256, 64)

	// 恢复后等到至少一帧，用于确认“恢复 -> 正常接续”。
	waitForSamples := func(after uint64, msg string) {
		t.Helper()
		waitFor(t, 2*time.Second, func() bool {
			return o.Stats().LatestSampleSeq > after
		}, msg)
	}

	const rounds = 12
	for i := 0; i < rounds; i++ {
		// 让流稳定采若干帧，并取挂起前最新序号作为冻结锚点。
		waitFor(t, 2*time.Second, func() bool {
			return o.Stats().LatestSampleSeq >= uint64(3*(i+1))
		}, "pre-suspend samples")
		frozen := o.Stats().LatestSampleSeq

		if err := h.Suspend(context.Background()); err != nil {
			t.Fatalf("round %d: suspend: %v", i, err)
		}

		// 至少积累若干次“终态 -> 重建流”周期，覆盖挂起落在采样过程
		// 不同时刻的各种调度情况（在途打断与挂起后重建都会命中）。
		waitFor(t, 2*time.Second, func() bool {
			nSuspended := 0
			for _, f := range o.RecentFailures(0) {
				if f.Code == CodeSuspended {
					nSuspended++
				}
			}
			return nSuspended >= 3*(i+1)
		}, "suspended failure records accumulate")

		// 挂起期间再观察一段：不得出现新采样（无陈旧帧），序号冻结。
		time.Sleep(30 * time.Millisecond)
		if seq := o.Stats().LatestSampleSeq; seq != frozen {
			t.Fatalf("round %d: samples advanced while suspended: %d -> %d",
				i, frozen, seq)
		}

		// 逐条归因检查：挂起相关记录只能是 SUSPENDED。
		fails := o.RecentFailures(0)
		if len(fails) == 0 {
			t.Fatalf("round %d: no failure records while suspended", i)
		}
		for _, f := range fails {
			if f.Code != CodeSuspended {
				t.Fatalf("round %d: failure attributed %s (%s), want SUSPENDED only; "+
					"设备侧挂起中断被误记为调用方主动结束或归因漂移",
					i, f.Code, f.Message)
			}
			if f.Op == "" || f.T.IsZero() || f.Seq == 0 {
				t.Fatalf("round %d: incomplete failure record: %+v", i, f)
			}
		}

		// 历史采样与失败记录在挂起期间仍能一起查到。
		if len(o.Recent(0)) == 0 {
			t.Fatalf("round %d: recent samples lost while suspended", i)
		}

		if err := h.Resume(context.Background()); err != nil {
			t.Fatalf("round %d: resume: %v", i, err)
		}
		waitForSamples(frozen, "sampling resumes after resume")
	}

	// 全程汇总：失败环内不得出现任何 CANCELED（比例恒定为 0）。
	var totalFails, suspended int
	for _, f := range o.RecentFailures(0) {
		totalFails++
		if f.Code == CodeSuspended {
			suspended++
		}
	}
	if totalFails == 0 || suspended != totalFails {
		t.Fatalf("attribution drifted: %d/%d failures are SUSPENDED, want all",
			suspended, totalFails)
	}

	// 恢复后全部保留帧按入环时间有序、序号连续，无乱序/陈旧帧混入。
	all := o.Recent(0)
	for j := 1; j < len(all); j++ {
		if all[j].Seq <= all[j-1].Seq {
			t.Fatalf("sample seq order broken across suspend/resume cycles: "+
				"%d after %d", all[j].Seq, all[j-1].Seq)
		}
		if !all[j].Sample.T.Equal(all[j-1].Sample.T) &&
			all[j].Sample.T.Before(all[j-1].Sample.T) {
			t.Fatalf("stale/out-of-order frame across resume: %s before %s",
				all[j].Sample.T, all[j-1].Sample.T)
		}
	}
	t.Logf("反复挂起/恢复 ×%d 轮：%d 条失败记录全部 SUSPENDED，0 条 CANCELED，"+
		"挂起期间零新帧，恢复后帧序号严格接续", rounds, totalFails)
}

// TestObserverSuspendUnderConcurrentQueries 并发访问下归因不退化：
// 挂起/恢复期间 8 路查询并发读取，采集与失败归因行为保持不变。
func TestObserverSuspendUnderConcurrentQueries(t *testing.T) {
	h, o := startObserver(t, 128, 64)
	waitFor(t, 2*time.Second, func() bool { return o.Stats().LatestSampleSeq >= 5 },
		"initial samples")

	stop := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var cursor uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				o.Recent(16)
				o.Since(cursor)
				o.RecentFailures(16)
				o.Stats()
			}
		}()
	}

	for i := 0; i < 6; i++ {
		if err := h.Suspend(context.Background()); err != nil {
			t.Fatalf("suspend %d: %v", i, err)
		}
		time.Sleep(25 * time.Millisecond)
		if err := h.Resume(context.Background()); err != nil {
			t.Fatalf("resume %d: %v", i, err)
		}
		time.Sleep(25 * time.Millisecond)
	}
	close(stop)
	wg.Wait()

	for _, f := range o.RecentFailures(0) {
		if f.Code != CodeSuspended {
			t.Fatalf("concurrent suspend cycle failure = %s, want SUSPENDED", f.Code)
		}
	}
	if st := o.Stats(); st.FailureCount > st.FailureCapacity {
		t.Fatalf("failure ring exceeded capacity: %+v", st)
	}
}

// TestShutdownAbortIsPoweredOff 关电路径的设备侧归因：
// Shutdown 打断在途采样必须记 POWERED_OFF，同样不能糊成 CANCELED。
func TestShutdownAbortIsPoweredOff(t *testing.T) {
	h, mb, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	blockStatusRead(mb, 40*time.Millisecond)

	readErr := make(chan error, 1)
	go func() { _, err := h.Read(ctx); readErr <- err }()
	time.Sleep(3 * time.Millisecond)
	if err := h.Shutdown(ctx); err != nil {
		t.Fatalf("shutdown: %v", err)
	}
	select {
	case err := <-readErr:
		mustCode(t, err, CodePoweredOff)
	case <-time.After(2 * time.Second):
		t.Fatal("aborted read never returned after shutdown")
	}
}

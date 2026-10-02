package hal

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
	"github.com/lisama569/sensor-hal-go/sensor"
)

// waitFor 轮询条件（测试侧短睡眠轮询，被测代码自身不忙等）。
func waitFor(t *testing.T, d time.Duration, cond func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("timeout waiting for: " + msg)
}

func startObserver(t *testing.T, capSamples, capFailures int) (*HAL, *Observer) {
	t.Helper()
	h, _, _ := newHAL(t)
	if err := h.Open(context.Background(), sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	o := NewObserver(h, ObserverConfig{
		SampleCapacity:  capSamples,
		FailureCapacity: capFailures,
		RestartWait:     20 * time.Millisecond,
	})
	o.Start(context.Background())
	t.Cleanup(o.Close)
	return h, o
}

func TestObserverBoundedCapacityStaysSteady(t *testing.T) {
	h, o := startObserver(t, 16, 4)
	// 让后台采到远超容量的帧数。
	waitFor(t, 2*time.Second, func() bool {
		return o.Stats().LatestSampleSeq >= 80
	}, "at least 80 samples collected")

	st := o.Stats()
	if st.SampleCount != 16 {
		t.Fatalf("SampleCount = %d, want capped at 16", st.SampleCount)
	}
	if st.SampleCapacity != 16 {
		t.Fatalf("SampleCapacity = %d", st.SampleCapacity)
	}
	if st.SampleDropped < 64 {
		t.Fatalf("SampleDropped = %d, want >= 64", st.SampleDropped)
	}
	if st.LatestSampleSeq < 80 {
		t.Fatalf("LatestSampleSeq = %d", st.LatestSampleSeq)
	}

	// 再跑一会儿：占用必须保持平稳，不随时间继续堆高。
	time.Sleep(40 * time.Millisecond)
	st2 := o.Stats()
	if st2.SampleCount != 16 {
		t.Fatalf("SampleCount grew to %d, want steady 16", st2.SampleCount)
	}
	if st2.LatestSampleSeq <= st.LatestSampleSeq {
		t.Fatalf("collector stalled: %d -> %d", st.LatestSampleSeq, st2.LatestSampleSeq)
	}

	// 保留窗口内帧严格按时间从老到新、序号连续。
	recent := o.Recent(0)
	if len(recent) != 16 {
		t.Fatalf("Recent len = %d, want 16", len(recent))
	}
	for i := 1; i < len(recent); i++ {
		if recent[i].Seq != recent[i-1].Seq+1 {
			t.Fatalf("seq gap at %d: %d after %d", i, recent[i].Seq, recent[i-1].Seq)
		}
		if !recent[i].Sample.T.After(recent[i-1].Sample.T) &&
			!recent[i].Sample.T.Equal(recent[i-1].Sample.T) {
			t.Fatalf("timestamps out of order: %s before %s",
				recent[i].Sample.T, recent[i-1].Sample.T)
		}
	}
	if recent[len(recent)-1].Seq != st2.LatestSampleSeq {
		t.Fatalf("latest recent seq %d != stats %d",
			recent[len(recent)-1].Seq, st2.LatestSampleSeq)
	}

	// Recent(max) 只取最新 max 条，且是独立拷贝。
	tail := o.Recent(5)
	if len(tail) != 5 || tail[0].Seq != recent[11].Seq {
		t.Fatalf("Recent(5) window wrong: %d entries, first seq %d",
			len(tail), tail[0].Seq)
	}
	tail[0] = LoggedSample{}
	if o.Recent(5)[0].Seq == 0 {
		t.Fatal("Recent returned slices that alias ring storage")
	}

	// 采集一直在继续：查询期间/之后仍能拿到更新的帧。
	before := o.Stats().LatestSampleSeq
	_ = h
	waitFor(t, time.Second, func() bool { return o.Stats().LatestSampleSeq > before },
		"collection continues after reads")
}

func TestObserverConfigDefaults(t *testing.T) {
	h, _, _ := newHAL(t)
	o := NewObserver(h, ObserverConfig{})
	st := o.Stats()
	if st.SampleCapacity != defaultSampleCapacity || st.FailureCapacity != defaultFailureCapacity {
		t.Fatalf("default capacities = %+v", st)
	}
	if o.cfg.RestartWait != defaultRestartWait {
		t.Fatalf("default restart wait = %v", o.cfg.RestartWait)
	}
}

func TestObserverSinceNoDataVsMissedVsCurrent(t *testing.T) {
	h, _, _ := newHAL(t)
	if err := h.Open(context.Background(), sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	o := NewObserver(h, ObserverConfig{
		SampleCapacity:  4,
		FailureCapacity: 4,
		RestartWait:     20 * time.Millisecond,
	})

	// 启动前、且从未采到任何帧：cursor=0 必须明确是 NO_DATA，而不是空糊成一种结果。
	b := o.Since(0)
	if b.Status != QueryNoData || len(b.Samples) != 0 {
		t.Fatalf("before start: status=%s samples=%d, want NO_DATA/0", b.Status, len(b.Samples))
	}
	b = o.Since(999)
	if b.Status != QueryNoData {
		t.Fatalf("future cursor with no data: %s, want NO_DATA", b.Status)
	}

	o.Start(context.Background())
	t.Cleanup(o.Close)

	// 取一帧当锚点。
	var first LoggedSample
	waitFor(t, 2*time.Second, func() bool {
		if r := o.Recent(1); len(r) == 1 {
			first = r[0]
			return true
		}
		return false
	}, "first sample")

	// cursor=0 在有数据后正常从头给。
	b = o.Since(0)
	if b.Status != QueryOK || len(b.Samples) == 0 || b.Samples[0].Seq != 1 {
		t.Fatalf("since(0) after data: status=%s n=%d", b.Status, len(b.Samples))
	}
	if b.OldestSeq != 1 {
		t.Fatalf("OldestSeq = %d, want 1", b.OldestSeq)
	}

	// 追到最新：CURRENT，与空结果明确区分。
	latest := b.Samples[len(b.Samples)-1].Seq
	b = o.Since(latest)
	if b.Status != QueryCurrent || len(b.Samples) != 0 {
		t.Fatalf("at tip: status=%s n=%d, want CURRENT/0", b.Status, len(b.Samples))
	}

	// 等窗口翻过去：锚点帧被挤掉 => MISSED，而不是像“没有新数据”一样返回空。
	waitFor(t, 2*time.Second, func() bool {
		return o.Since(first.Seq).Status == QueryMissed
	}, "first frame evicted")
	b = o.Since(first.Seq)
	if b.Status != QueryMissed {
		t.Fatalf("status = %s, want MISSED", b.Status)
	}
	if len(b.Samples) == 0 {
		t.Fatal("MISSED must still return the retained window, got empty")
	}
	if b.Samples[0].Seq != b.OldestSeq || b.Samples[len(b.Samples)-1].Seq != b.LatestSeq {
		t.Fatal("MISSED batch does not cover the stated window")
	}
	if b.OldestSeq <= first.Seq {
		t.Fatalf("window oldest %d should be > evicted cursor %d", b.OldestSeq, first.Seq)
	}

	// 游标落在窗口中间：OK，只给严格更新的帧，且首帧序号 == cursor+1。
	mid := b.OldestSeq + 1
	b = o.Since(mid)
	if b.Status != QueryOK || len(b.Samples) == 0 {
		t.Fatalf("in-window cursor: status=%s n=%d", b.Status, len(b.Samples))
	}
	if b.Samples[0].Seq != mid+1 {
		t.Fatalf("first returned seq %d, want %d", b.Samples[0].Seq, mid+1)
	}
}

func TestObserverFramesAreCompleteAndOrdered(t *testing.T) {
	_, o := startObserver(t, 32, 4)
	waitFor(t, 2*time.Second, func() bool { return o.Stats().LatestSampleSeq >= 40 },
		"40+ samples")
	all := o.Recent(0)
	if len(all) != 32 {
		t.Fatalf("retained = %d, want 32", len(all))
	}
	var prevDeviceSeq int16
	for i, ls := range all {
		// 观测环序号在缓冲窗口内必须连续（没有半帧被单独计入或漏记）。
		if i > 0 && ls.Seq != all[i-1].Seq+1 {
			t.Fatalf("logged seq not contiguous: %d after %d", ls.Seq, all[i-1].Seq)
		}
		// 每一帧的三轴必须共同对应同一个设备帧号：模拟设备 triad(n)
		// = (n, 2n, 1000-3n)。若读到撕裂/半帧（如新旧两轴拼装），
		// 三轴解出的帧号会对不上。
		n := ls.Sample.X
		if ls.Sample.Y != n*2 {
			t.Fatalf("frame logged=%d torn: x=%d but y=%d (want %d)",
				ls.Seq, n, ls.Sample.Y, n*2)
		}
		if ls.Sample.Z != 1000-n*3 {
			t.Fatalf("frame logged=%d torn: x=%d but z=%d (want %d)",
				ls.Seq, n, ls.Sample.Z, 1000-n*3)
		}
		if n <= prevDeviceSeq {
			t.Fatalf("device frames not strictly increasing: %d after %d", n, prevDeviceSeq)
		}
		prevDeviceSeq = n
		if ls.Sample.ODR != sensor.ODR400 || ls.Sample.FSR != sensor.FSR4 {
			t.Fatalf("frame %d config snapshot mismatch: %+v", ls.Seq, ls.Sample)
		}
		if ls.Sample.T.IsZero() {
			t.Fatalf("frame %d zero timestamp", ls.Seq)
		}
	}
}

// failingHAL 构造一个“设备在就绪窗口内一直不准备好”的环境：
// 状态寄存器读永远总线超时，且驱动采样窗口收紧到 20ms，使失败快速、确定。
func failingHAL(t *testing.T) (*HAL, *Observer) {
	t.Helper()
	h, mb, _ := newHAL(t)
	if err := h.Open(context.Background(), sensor.Config{ODR: sensor.ODR100, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	mb.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: sensor.AddrStatus, Fault: bus.FaultTimeout})
	o := NewObserver(h, ObserverConfig{
		SampleCapacity:  4,
		FailureCapacity: 8,
		RestartWait:     5 * time.Millisecond,
	})
	return h, o
}

func TestObserverRecordsTimeoutFailuresStably(t *testing.T) {
	h, o := failingHAL(t)

	// 直接路径：同样的设备行为，连续多次结论都必须是 TIMEOUT，
	// 不随“内部截止是否恰好打断某次读”的调度时机翻成 CANCELED。
	for i := 0; i < 10; i++ {
		_, err := h.Read(context.Background())
		mustCode(t, err, CodeTimeout)
	}

	o.Start(context.Background())
	t.Cleanup(o.Close)
	waitFor(t, 2*time.Second, func() bool { return len(o.RecentFailures(0)) >= 5 },
		"failure records accumulated")

	fails := o.RecentFailures(0)
	for _, f := range fails {
		if f.Code != CodeTimeout {
			t.Fatalf("failure code = %s (%s), want stable TIMEOUT", f.Code, f.Message)
		}
		if f.T.IsZero() || f.Op == "" || f.Seq == 0 {
			t.Fatalf("failure record incomplete: %+v", f)
		}
	}
	// 失败序号单调递增、彼此不同，便于与采样按时间对齐复盘。
	for i := 1; i < len(fails); i++ {
		if fails[i].Seq <= fails[i-1].Seq {
			t.Fatalf("failure seqs not increasing: %d after %d", fails[i].Seq, fails[i-1].Seq)
		}
	}
	// 失败期间不应产生任何采样。
	if st := o.Stats(); st.LatestSampleSeq != 0 {
		t.Fatalf("no samples expected while never ready, got seq %d", st.LatestSampleSeq)
	}
}

func TestObserverTimeoutDistinctFromCallerCancel(t *testing.T) {
	h, o := failingHAL(t)
	o.Start(context.Background())
	t.Cleanup(o.Close)
	waitFor(t, 2*time.Second, func() bool { return len(o.RecentFailures(0)) >= 1 },
		"timeout recorded")

	// 调用方主动取消自己的单次采样：结论必须是 CANCELED，与设备不就绪的
	// TIMEOUT 明确分开；这是调用方行为，也不应混进失败记录（流用的是
	// Observer 自己的 ctx，与本次取消无关）。
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := h.Read(ctx)
	mustCode(t, err, CodeCanceled)

	time.Sleep(20 * time.Millisecond)
	for _, f := range o.RecentFailures(0) {
		if f.Code != CodeTimeout {
			t.Fatalf("caller cancel leaked into failure log as %s", f.Code)
		}
	}
}

func TestObserverFailureLogBounded(t *testing.T) {
	_, o := failingHAL(t)
	o.Start(context.Background())
	t.Cleanup(o.Close)
	waitFor(t, 2*time.Second, func() bool {
		st := o.Stats()
		return st.FailureCount == 8 && st.FailureDropped > 0
	}, "failure ring saturated")
	st := o.Stats()
	if st.FailureCount != 8 || st.FailureCapacity != 8 {
		t.Fatalf("failure ring not capped: %+v", st)
	}
	fails := o.RecentFailures(0)
	if len(fails) != 8 {
		t.Fatalf("RecentFailures len = %d, want 8", len(fails))
	}
}

func TestObserverQueriesDoNotBlockCollection(t *testing.T) {
	_, o := startObserver(t, 64, 8)
	waitFor(t, 2*time.Second, func() bool { return o.Stats().LatestSampleSeq >= 10 },
		"some samples")

	// 一边高频做各类查询，一边采集必须持续推进：查询既不打断采集，
	// 也不需要采集方等待（采集 goroutine 不在查询锁上做任何等待）。
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			var cursor uint64
			for {
				select {
				case <-stop:
					return
				default:
				}
				o.Recent(8)
				o.Since(cursor)
				o.RecentFailures(4)
				o.Stats()
			}
		}()
	}

	start := o.Stats().LatestSampleSeq
	waitFor(t, 2*time.Second, func() bool { return o.Stats().LatestSampleSeq >= start+20 },
		"collection advances under query load")
	close(stop)
	readers.Wait()
}

func TestObserverSuspendResumeKeepsHistory(t *testing.T) {
	h, o := startObserver(t, 128, 16)
	waitFor(t, 2*time.Second, func() bool { return o.Stats().LatestSampleSeq >= 5 },
		"pre-suspend samples")
	beforeSuspend := o.Stats().LatestSampleSeq

	if err := h.Suspend(context.Background()); err != nil {
		t.Fatal(err)
	}
	// 挂起期间流以终态错误结束、按 RestartWait 周期重建并记录失败。
	waitFor(t, 2*time.Second, func() bool { return len(o.RecentFailures(0)) >= 2 },
		"failure recorded while suspended")
	for _, f := range o.RecentFailures(0) {
		// 挂起是设备侧行为：失败归因必须稳定为 SUSPENDED，绝不能因调度
		// 时机（取消恰好打断在途读 vs. 挂起后状态检查拒绝）飘成 CANCELED。
		if f.Code != CodeSuspended {
			t.Fatalf("suspend failure code = %s, want stably SUSPENDED", f.Code)
		}
		if f.Code == CodeCanceled {
			t.Fatalf("device-side suspend misclassified as caller cancel: %+v", f)
		}
	}
	// 挂起期间不会产生任何新采样（不返回陈旧数据）。
	time.Sleep(30 * time.Millisecond)
	if seq := o.Stats().LatestSampleSeq; seq != beforeSuspend {
		t.Fatalf("samples advanced while suspended: %d -> %d", beforeSuspend, seq)
	}
	// 历史采样仍可查询，供与失败记录一起复盘。
	if len(o.Recent(0)) == 0 {
		t.Fatal("recent samples lost during suspend")
	}

	if err := h.Resume(context.Background()); err != nil {
		t.Fatal(err)
	}
	waitFor(t, 2*time.Second, func() bool {
		return o.Stats().LatestSampleSeq > beforeSuspend
	}, "sampling resumes after resume")

	// 恢复后帧序号严格接续、不重复、不倒退（没有陈旧帧混入）。
	all := o.Recent(0)
	for i := 1; i < len(all); i++ {
		if all[i].Seq <= all[i-1].Seq {
			t.Fatalf("frame seq order broken across resume: %d after %d",
				all[i].Seq, all[i-1].Seq)
		}
	}
}

// TestObserverSuspendResumeAttributionStableAcrossCycles 反复执行挂起/恢复
// 压测，断言设备侧中断的失败归因在所有轮次、所有调度时机下都稳定为
// SUSPENDED，从不被误记成调用方主动结束的 CANCELED；同时覆盖触发边界：
// 挂起期间不产新帧/不吐陈旧帧，恢复后帧序号严格接续，失败与最近采样可一起查。
func TestObserverSuspendResumeAttributionStableAcrossCycles(t *testing.T) {
	h, o := startObserver(t, 256, 256)
	ctx := context.Background()

	const cycles = 12
	var prevLatest uint64
	for c := 1; c <= cycles; c++ {
		// 恢复后（首轮为启动后）等后台采到至少若干帧，让 Suspend 可能落在
		// 采集过程的不同时刻：总线读在途、轮询休眠间隙、两帧之间等。
		want := prevLatest + 3
		waitFor(t, 2*time.Second, func() bool {
			return o.Stats().LatestSampleSeq >= want
		}, "pre-suspend samples cycle")
		beforeSuspend := o.Stats().LatestSampleSeq

		failsBefore := len(o.RecentFailures(0))
		if err := h.Suspend(ctx); err != nil {
			t.Fatalf("cycle %d suspend: %v", c, err)
		}

		// 保持挂起若干个重建周期：泵会反复建流、反复得到终态错误。
		// 每一轮重建流时，挂起可能打断在途读（旧代码记 CANCELED），
		// 也可能在状态检查处被拒（记 SUSPENDED）——归因必须一致。
		time.Sleep(o.cfg.RestartWait*4 + 25*time.Millisecond)

		failsDuring := o.RecentFailures(0)[failsBefore:]
		if len(failsDuring) == 0 {
			t.Fatalf("cycle %d: no failure recorded while suspended", c)
		}
		for _, f := range failsDuring {
			if f.Code != CodeSuspended {
				t.Fatalf("cycle %d unstable attribution: failure #%d code=%s (%s), "+
					"want SUSPENDED for device-side suspend in every cycle",
					c, f.Seq, f.Code, f.Message)
			}
			if f.Op != "sample" || f.T.IsZero() || f.Seq == 0 {
				t.Fatalf("cycle %d incomplete failure record: %+v", c, f)
			}
		}
		t.Logf("cycle %2d: suspended, %2d terminal failures all SUSPENDED "+
			"(latest sample seq=%d frozen)", c, len(failsDuring), beforeSuspend)

		// 触发边界 1：挂起期间绝不产生新采样，也不会吐出陈旧/乱序帧。
		if seq := o.Stats().LatestSampleSeq; seq != beforeSuspend {
			t.Fatalf("cycle %d: samples advanced while suspended: %d -> %d",
				c, beforeSuspend, seq)
		}
		recent := o.Recent(0)
		if len(recent) == 0 {
			t.Fatalf("cycle %d: recent samples lost while suspended", c)
		}
		if recent[len(recent)-1].Seq != beforeSuspend {
			t.Fatalf("cycle %d: stale/extra frame appeared in recent window", c)
		}
		// 失败记录与最近采样必须能一起查到（同一快照视角复盘）。
		if st := o.Stats(); st.FailureCount != len(o.RecentFailures(0)) {
			t.Fatalf("cycle %d: failure stats/snapshot disagree", c)
		}

		if err := h.Resume(ctx); err != nil {
			t.Fatalf("cycle %d resume: %v", c, err)
		}
		// 触发边界 2：恢复后采样正常接续，序号严格大于挂起前最后一帧。
		waitFor(t, 2*time.Second, func() bool {
			return o.Stats().LatestSampleSeq > beforeSuspend
		}, "samples resume after resume")
		all := o.Recent(0)
		for i := 1; i < len(all); i++ {
			if all[i].Seq <= all[i-1].Seq {
				t.Fatalf("cycle %d: seq order broken: %d after %d",
					c, all[i].Seq, all[i-1].Seq)
			}
		}
		if all[len(all)-1].Seq != o.Stats().LatestSampleSeq {
			t.Fatalf("cycle %d: latest recent frame disagrees with stats", c)
		}
		prevLatest = o.Stats().LatestSampleSeq
		t.Logf("cycle %2d: resumed, samples continued seq -> %d", c, prevLatest)
	}

	// 全程汇总：失败环里不允许出现任何一条 CANCELED——Close 尚未发生，
	// 调用方从未取消自己的采集，所有设备侧中断都必须是 SUSPENDED。
	for _, f := range o.RecentFailures(0) {
		if f.Code == CodeCanceled {
			t.Fatalf("device-side suspend interrupt misrecorded as caller cancel: %+v", f)
		}
	}
}

// TestObserverSuspendAttributionIndependentOfTiming 直接对 HAL.Read 做挂起
// 压测：把挂起插入采样过程的不同时刻（在途总线读 / 就绪轮询等待 / 即将发读），
// 反复多轮，结论必须轮轮相同且恒为 SUSPENDED；调用方主动取消则恒为 CANCELED。
func TestObserverSuspendAttributionIndependentOfTiming(t *testing.T) {
	h, mb, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}

	// 给状态寄存器读加一个可被取消打断的延迟，制造“取消恰好打断在途总线读”
	// 的时机；每轮只生效一次，恢复清障后采样可正常完成。
	const rounds = 25
	for i := 0; i < rounds; i++ {
		mb.AddFault(bus.FaultRule{
			Kind: bus.OpRead, Addr: sensor.AddrStatus,
			Fault: bus.FaultTimeout, Delay: 30 * time.Millisecond, Times: 1,
		})
		errCh := make(chan error, 1)
		go func() {
			_, e := h.Read(ctx)
			errCh <- e
		}()
		// 在 [0,30ms) 内分散挂起点：早取消打断在途读，晚取消与读超时竞争。
		// 结论不能因为这个早晚差异而改变。
		time.Sleep(time.Duration(i%6) * 4 * time.Millisecond)
		if err := h.Suspend(ctx); err != nil {
			t.Fatalf("round %d suspend: %v", i, err)
		}
		select {
		case err := <-errCh:
			mustCode(t, err, CodeSuspended)
		case <-time.After(time.Second):
			t.Fatalf("round %d: in-flight read never returned", i)
		}
		mb.ClearFaults()
		if err := h.Resume(ctx); err != nil {
			t.Fatalf("round %d resume: %v", i, err)
		}
		// 恢复后下一轮前先确认采样恢复正常（不返回陈旧帧）。
		if _, err := h.Read(ctx); err != nil {
			t.Fatalf("round %d post-resume read: %v", i, err)
		}
	}
	t.Logf("completed %d suspend-at-various-timing rounds, all SUSPENDED", rounds)

	// 同一设备、同样的在途形态，由调用方取消时结论恒为 CANCELED。
	mb.AddFault(bus.FaultRule{
		Kind: bus.OpRead, Addr: sensor.AddrStatus,
		Fault: bus.FaultTimeout, Delay: 100 * time.Millisecond,
	})
	for i := 0; i < 5; i++ {
		cctx, cancel := context.WithCancel(ctx)
		errCh := make(chan error, 1)
		go func() {
			_, e := h.Read(cctx)
			errCh <- e
		}()
		time.Sleep(5 * time.Millisecond)
		cancel()
		select {
		case err := <-errCh:
			mustCode(t, err, CodeCanceled)
		case <-time.After(time.Second):
			t.Fatal("caller-canceled read never returned")
		}
	}
}

func TestObserverCloseStopsPump(t *testing.T) {
	h, o := startObserver(t, 32, 8)
	waitFor(t, 2*time.Second, func() bool { return o.Stats().LatestSampleSeq >= 5 },
		"samples before close")

	// Close 必须等待采集 goroutine 真正退出；之后序号冻结。
	o.Close()
	seq := o.Stats().LatestSampleSeq
	time.Sleep(40 * time.Millisecond)
	if got := o.Stats().LatestSampleSeq; got != seq {
		t.Fatalf("pump did not stop: seq %d -> %d", seq, got)
	}
	// 幂等：再次 Close 安全；Close 不改设备电源状态。
	o.Close()
	if h.State() != sensor.StateActive {
		t.Fatalf("Close changed device state to %s", h.State())
	}
	// 停止后查询仍可正常返回最后的快照。
	if len(o.Recent(0)) == 0 {
		t.Fatal("history unavailable after close")
	}
}

func TestObserverNoGoroutineLeakAcrossRestarts(t *testing.T) {
	h, o := startObserver(t, 16, 16)
	// 通过若干次挂起/恢复，强制 run 反复重建 Stream；
	// 每个 Stream goroutine 都应在流关闭后退出。
	for i := 0; i < 4; i++ {
		if err := h.Suspend(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
		if err := h.Resume(context.Background()); err != nil {
			t.Fatal(err)
		}
		time.Sleep(30 * time.Millisecond)
	}

	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	base := runtime.NumGoroutine()

	o.Close()
	runtime.GC()
	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		t.Fatalf("goroutine leak after close: base=%d now=%d", base, n)
	}
}

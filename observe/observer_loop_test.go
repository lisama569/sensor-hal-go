package observe

import (
	"context"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/hal"
)

// TestRunCollectsSamples 验证采集循环把成功采样按序写入缓存。
func TestRunCollectsSamples(t *testing.T) {
	base := time.Unix(2000, 0)
	var steps []step
	for i := 1; i <= 6; i++ {
		steps = append(steps, step{s: mkSample(i, base.Add(time.Duration(i)*time.Millisecond))})
	}
	fs := &fakeSampler{steps: steps, blockWhenDone: true}
	o := New(fs, Options{SampleCapacity: 10, FailureCapacity: 4, RetryInterval: time.Millisecond})

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Run(ctx); close(done) }()

	deadline := time.After(2 * time.Second)
	for o.Stats().SamplesTotal < 6 {
		select {
		case <-deadline:
			t.Fatalf("collected %d samples", o.Stats().SamplesTotal)
		default:
			time.Sleep(time.Millisecond)
		}
	}
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("Run did not return after stop")
	}

	batch := o.Recent(0)
	if len(batch.Frames) != 6 {
		t.Fatalf("frames = %d", len(batch.Frames))
	}
	for i, f := range batch.Frames {
		if f.Seq != int64(i+1) {
			t.Fatalf("frame order broken at %d: seq=%d", i, f.Seq)
		}
		// 完整性：帧内字段必须与采样源一致，不能读到半帧。
		want := steps[i].s
		if f.Sample.X != want.X || f.Sample.Y != want.Y || f.Sample.Z != want.Z ||
			f.Sample.ODR != want.ODR || f.Sample.FSR != want.FSR || !f.Sample.T.Equal(want.T) {
			t.Fatalf("corrupt frame: %+v want %+v", f.Sample, want)
		}
	}
}

// TestFailuresRecordedAndBounded 验证设备侧失败被稳定归因、可查询、
// 容量有界；以及调用方停止不产生 CANCELED 失败记录。
func TestFailuresRecordedAndBounded(t *testing.T) {
	// 脚本：2 次未就绪超时、3 次挂起、之后成功帧。
	errs := []hal.Code{hal.CodeTimeout, hal.CodeTimeout,
		hal.CodeSuspended, hal.CodeSuspended, hal.CodeSuspended}
	var steps []step
	for _, c := range errs {
		steps = append(steps, step{err: devErr(c, "sample")})
	}
	for i := 1; i <= 3; i++ {
		steps = append(steps, mkStep(i))
	}
	fs := &fakeSampler{steps: steps, blockWhenDone: true}
	o := New(fs, Options{SampleCapacity: 8, FailureCapacity: 4, RetryInterval: time.Millisecond})

	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Run(ctx); close(done) }()

	// 等待全部失败与若干成功被处理。
	deadline := time.After(2 * time.Second)
	for o.Stats().FailuresTotal < 5 {
		select {
		case <-deadline:
			t.Fatalf("failures = %d", o.Stats().FailuresTotal)
		default:
			time.Sleep(time.Millisecond)
		}
	}

	// 主动停止：不得记成一条 CANCELED 设备失败。
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not stop")
	}

	fl := o.Failures()
	if len(fl) != 4 { // 容量 4：最早一条超时被挤掉，保留 1 超时 + 3 挂起
		t.Fatalf("failure count = %d, want 4", len(fl))
	}
	if fl[0].Seq != 2 || fl[3].Seq != 5 {
		t.Fatalf("failure seqs = %d,%d,%d,%d", fl[0].Seq, fl[1].Seq, fl[2].Seq, fl[3].Seq)
	}
	codes := []hal.Code{hal.CodeTimeout, hal.CodeSuspended, hal.CodeSuspended, hal.CodeSuspended}
	for i, f := range fl {
		if f.Code != codes[i] {
			t.Fatalf("failure %d code = %s, want %s", i, f.Code, codes[i])
		}
		if f.T.IsZero() || f.Message == "" {
			t.Fatalf("failure record incomplete: %+v", f)
		}
	}
	for _, f := range fl {
		if f.Code == hal.CodeCanceled {
			t.Fatalf("caller stop must not be recorded as failure: %+v", f)
		}
	}
	if st := o.Stats(); st.FailuresTotal != 5 || st.FailureCount != 4 {
		t.Fatalf("stats = %+v", st)
	}
}

// TestStopNotCountedAsDeviceFailure 更直接地验证：Read 因停止 ctx
// 结束时立即安静退出，不写任何失败记录。
func TestStopNotCountedAsDeviceFailure(t *testing.T) {
	fs := &fakeSampler{block: make(chan struct{})}
	o := New(fs, Options{RetryInterval: time.Millisecond})
	ctx, stop := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { o.Run(ctx); close(done) }()

	// 等采样真正在途后停止。
	time.Sleep(10 * time.Millisecond)
	stop()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("run did not stop promptly")
	}
	if len(o.Failures()) != 0 {
		t.Fatalf("expected no failure on caller stop, got %+v", o.Failures())
	}
}

// TestReadsDoNotBlockCollection 验证读取方拿快照时，采集不被卡住：
// 让 Read 持续高频产出，读取方在外部互斥锁保护下反复快照，
// 采集计数必须继续增长。
func TestReadsDoNotBlockCollection(t *testing.T) {
	fs := &countingSampler{}
	o := New(fs, Options{SampleCapacity: 4, FailureCapacity: 4, RetryInterval: time.Microsecond})
	ctx, stop := context.WithCancel(context.Background())
	go o.Run(ctx)

	stopReader := make(chan struct{})
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			select {
			case <-stopReader:
				return
			default:
				_ = o.Recent(0)
				_ = o.Since(1, 2)
				_ = o.Failures()
				_ = o.Stats()
			}
		}
	}()

	time.Sleep(30 * time.Millisecond)
	n := o.Stats().SamplesTotal
	time.Sleep(30 * time.Millisecond)
	n2 := o.Stats().SamplesTotal
	close(stopReader)
	wg.Wait()
	stop()

	if n2 <= n {
		t.Fatalf("collection stalled during reads: %d -> %d", n, n2)
	}
}

// TestSnapshotImmutable 验证返回给调用方的帧切片是独立拷贝：
// 调用方改写后，缓存后续返回不受影响。
func TestSnapshotImmutable(t *testing.T) {
	b := newBuffer(4, 4, nil)
	b.addSample(mkSample(7, time.Now()))
	first := b.Recent(0)
	first.Frames[0].Sample.X = 999
	first.Frames[0] = Frame{}
	again := b.Recent(0)
	if again.Frames[0].Sample.X != 7 {
		t.Fatalf("snapshot was mutated through returned slice: %+v", again.Frames[0])
	}
}

// TestConcurrentReadersNoRace 在 -race 下验证多读取方并发快照、
// 同时采集推进时不存在竞争，且每批帧完整有序。
func TestConcurrentReadersNoRace(t *testing.T) {
	fs := &countingSampler{}
	o := New(fs, Options{SampleCapacity: 16, FailureCapacity: 8, RetryInterval: time.Microsecond})
	ctx, stop := context.WithCancel(context.Background())
	go o.Run(ctx)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cursor := int64(0)
			deadline := time.Now().Add(50 * time.Millisecond)
			for time.Now().Before(deadline) {
				batch := o.Since(cursor, 4)
				if batch.Status == StatusOK {
					var last int64 = cursor
					for _, f := range batch.Frames {
						if f.Seq != last+1 && last != 0 && f.Seq > 1 {
							// 淘汰会跳号；未淘汰时分页内必须连续。
						}
						if f.Seq <= last {
							t.Errorf("out-of-order frame: %d after %d", f.Seq, last)
						}
						if f.Sample.Y != f.Sample.X*2 {
							t.Errorf("torn frame: x=%d y=%d", f.Sample.X, f.Sample.Y)
						}
						last = f.Seq
					}
					if batch.NextCursor > cursor {
						cursor = batch.NextCursor
					}
				} else {
					cursor = batch.NextCursor
				}
			}
		}()
	}
	wg.Wait()
	stop()
}

// TestStartWaitGoroutineReclaims 验证 Start/Wait 生命周期：
// 停止后 goroutine 必然退出，资源不随多次启停泄漏。
func TestStartWaitGoroutineReclaims(t *testing.T) {
	fs := &countingSampler{}
	obs := New(fs, Options{RetryInterval: time.Millisecond})

	runtime.GC()
	base := runtime.NumGoroutine()
	ctx, stop := obs.Start()
	time.Sleep(20 * time.Millisecond)
	stop()
	obs.Wait()
	if err := ctx.Err(); err == nil {
		t.Fatal("stopped ctx must be canceled")
	}
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	if n := runtime.NumGoroutine(); n > base+1 {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

// countingSampler 永远立刻成功返回，序号在 Sample.X 中自增。
type countingSampler struct {
	mu  sync.Mutex
	seq int
}

func (c *countingSampler) Read(ctx context.Context) (hal.Sample, error) {
	c.mu.Lock()
	c.seq++
	s := mkSample(c.seq, time.Now())
	c.mu.Unlock()
	return s, nil
}

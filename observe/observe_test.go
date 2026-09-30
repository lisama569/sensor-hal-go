package observe

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/hal"
	"github.com/lisama569/sensor-hal-go/sensor"
)

// step 是脚本中的一次调用结果：err 非 nil 时返回失败，否则返回 s。
type step struct {
	s   sensor.Sample
	err error
}

// fakeSampler 是确定性的采样源：按 steps 脚本依次返回结果，
// 不碰真实总线，可精确控制每一次调用。
type fakeSampler struct {
	mu    sync.Mutex
	steps []step
	idx   int
	calls int
	// block 非 nil 时 Read 在取脚本之前阻塞直到它关闭或 ctx 结束。
	block chan struct{}
	// blockWhenDone 为 true 时，脚本耗尽后阻塞至 ctx 结束，
	// 不再产生额外结果。
	blockWhenDone bool
}

func (f *fakeSampler) Read(ctx context.Context) (hal.Sample, error) {
	f.mu.Lock()
	block := f.block
	idx := f.idx
	f.idx++
	f.calls++
	done := f.blockWhenDone && idx >= len(f.steps)
	var st step
	if !done && idx < len(f.steps) {
		st = f.steps[idx]
	}
	f.mu.Unlock()

	if block != nil {
		select {
		case <-block:
		case <-ctx.Done():
			return hal.Sample{}, ctx.Err()
		}
	}
	if done {
		<-ctx.Done()
		return hal.Sample{}, ctx.Err()
	}
	if st.err != nil {
		return hal.Sample{}, st.err
	}
	return st.s, nil
}

func mkStep(seq int) step { return step{s: mkSample(seq, time.Unix(int64(1000+seq), 0))} }

func mkSample(seq int, t time.Time) sensor.Sample {
	return sensor.Sample{T: t, X: int16(seq), Y: int16(seq * 2), Z: int16(1000 - seq*3),
		Quality: sensor.QOK, ODR: sensor.ODR100, FSR: sensor.FSR4}
}

func devErr(code hal.Code, op string) error {
	return &hal.DeviceError{Code: code, Op: op, Message: code.String() + " (test)"}
}

// TestBufferEvictionKeepsBounds 验证容量上限与 FIFO 淘汰：
// 写入远超容量后，只保留最新的 cap 条，内存占用平稳。
func TestBufferEvictionKeepsBounds(t *testing.T) {
	const cap = 4
	now := time.Unix(1000, 0)
	clk := func() time.Time { return now }
	b := newBuffer(cap, cap, clk)
	for i := 1; i <= 10; i++ {
		b.addSample(mkSample(i, now))
	}
	st := b.Stats()
	if st.SampleCount != cap || st.SamplesTotal != 10 || st.LatestSeq != 10 {
		t.Fatalf("unexpected stats: %+v", st)
	}
	batch := b.Recent(0)
	if batch.Status != StatusOK || len(batch.Frames) != cap {
		t.Fatalf("recent: status=%s n=%d", batch.Status, len(batch.Frames))
	}
	for i, f := range batch.Frames {
		wantSeq := int64(10 - cap + 1 + i)
		if f.Seq != wantSeq {
			t.Fatalf("pos %d: want seq %d, got %d", i, wantSeq, f.Seq)
		}
		if f.Sample.X != int16(wantSeq) {
			t.Fatalf("frame content mismatch at seq %d: %+v", wantSeq, f.Sample)
		}
		if f.Stored != now {
			t.Fatalf("stored time not set")
		}
	}
	if batch.OldestSeq != 7 || batch.LatestSeq != 10 || batch.NextCursor != 10 {
		t.Fatalf("bounds: %+v", batch)
	}
}

// TestRecentLimitsN 验证 Recent(n) 只取最近 n 条。
func TestRecentLimitsN(t *testing.T) {
	b := newBuffer(10, 10, nil)
	for i := 1; i <= 5; i++ {
		b.addSample(mkSample(i, time.Now()))
	}
	batch := b.Recent(2)
	if len(batch.Frames) != 2 || batch.Frames[0].Seq != 4 || batch.Frames[1].Seq != 5 {
		t.Fatalf("recent(2) = %+v", batch.Frames)
	}
}

// TestEmptyVsEvicted 验证"从无采样"与"游标数据被挤掉"可区分。
func TestEmptyVsEvicted(t *testing.T) {
	b := newBuffer(3, 3, nil)

	// 从无采样：Recent 与 Since 都必须是 StatusEmpty，而不是空结果糊掉。
	if got := b.Recent(10); got.Status != StatusEmpty {
		t.Fatalf("empty recent status = %s", got.Status)
	}
	if got := b.Since(0, 10); got.Status != StatusEmpty {
		t.Fatalf("empty since status = %s", got.Status)
	}

	// 写入 5 帧，容量 3 => 保留 seq 3,4,5，seq 1,2 被挤掉。
	for i := 1; i <= 5; i++ {
		b.addSample(mkSample(i, time.Now()))
	}
	if got := b.Since(2, 10); got.Status != StatusEvicted {
		t.Fatalf("want EVICTED for stale cursor, got %s", got.Status)
	}
	// 恰好指向最老帧之前（cursor=Oldest-1）也是被挤掉。
	if got := b.Since(2, 10); got.OldestSeq != 3 {
		t.Fatalf("oldest = %d", got.OldestSeq)
	}
	// cursor 落在窗口内：正常增量，且只返回游标之后的帧。
	got := b.Since(3, 10)
	if got.Status != StatusOK {
		t.Fatalf("status = %s", got.Status)
	}
	if len(got.Frames) != 2 || got.Frames[0].Seq != 4 || got.Frames[1].Seq != 5 {
		t.Fatalf("frames = %+v", got.Frames)
	}
	if got.NextCursor != 5 {
		t.Fatalf("next cursor = %d", got.NextCursor)
	}
	// 游标已是最新：OK + 空增量，不报错。
	again := b.Since(5, 10)
	if again.Status != StatusOK || len(again.Frames) != 0 {
		t.Fatalf("want OK empty delta, got %s n=%d", again.Status, len(again.Frames))
	}
}

// TestSinceMaxLimit 验证增量读取的条数上限与分批续读。
func TestSinceMaxLimit(t *testing.T) {
	b := newBuffer(10, 10, nil)
	for i := 1; i <= 5; i++ {
		b.addSample(mkSample(i, time.Now()))
	}
	first := b.Since(0, 2)
	if len(first.Frames) != 2 || first.NextCursor != 2 {
		t.Fatalf("first page = %+v", first)
	}
	second := b.Since(first.NextCursor, 2)
	if len(second.Frames) != 2 || second.Frames[0].Seq != 3 || second.NextCursor != 4 {
		t.Fatalf("second page = %+v", second)
	}
	third := b.Since(second.NextCursor, 2)
	if len(third.Frames) != 1 || third.Frames[0].Seq != 5 || third.NextCursor != 5 {
		t.Fatalf("third page = %+v", third)
	}
}

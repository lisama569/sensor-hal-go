package hal

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
	"github.com/lisama569/sensor-hal-go/sensor"
)

type tLogger struct{ t *testing.T }

func (l tLogger) Printf(format string, args ...any) { l.t.Logf(format, args...) }

func newHAL(t *testing.T) (*HAL, *bus.MemBus, *sensor.SimDevice) {
	t.Helper()
	regs := map[uint8]uint16{
		sensor.AddrChipID:  sensor.ChipIDA,
		sensor.AddrVersion: sensor.EncodeVersion(sensor.RegVersion{Major: 2, Minor: 1}),
	}
	mb := bus.NewMemBus(regs, nil)
	sim := sensor.NewSimDevice(nil)
	mb.SetModel(sim)
	opts := sensor.DefaultOptions()
	drv := sensor.New(mb, sensor.RegVersion{Major: 2, Minor: 1}, tLogger{t}, opts)
	return New(drv), mb, sim
}

func mustCode(t *testing.T, err error, want Code) *DeviceError {
	t.Helper()
	var de *DeviceError
	if !errors.As(err, &de) {
		t.Fatalf("want *DeviceError, got %T: %v", err, err)
	}
	if de.Code != want {
		t.Fatalf("want code %s, got %s (%v)", want, de.Code, err)
	}
	t.Logf("HAL 错误码符合预期: %v", de)
	return de
}

func TestOpenRead(t *testing.T) {
	h, mb, _ := newHAL(t)
	defer func() {
		for _, e := range mb.Log() {
			t.Logf("BUS #%d %s addr=0x%02X vals=%#x fault=%s", e.Seq, e.Kind, e.Addr, e.Values, e.Fault)
		}
	}()
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR100, FSR: sensor.FSR4}); err != nil {
		t.Fatalf("open: %v", err)
	}
	s, err := h.Read(ctx)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if s.T.IsZero() || s.ODR != sensor.ODR100 {
		t.Fatalf("bad sample: %+v", s)
	}
	t.Logf("采样: t=%s x=%d y=%d z=%d q=%s", s.T.Format(time.RFC3339Nano), s.X, s.Y, s.Z, s.Quality)
}

func TestOpenBadConfig(t *testing.T) {
	h, _, _ := newHAL(t)
	err := h.Open(context.Background(), sensor.Config{ODR: sensor.Hz(99), FSR: sensor.FSR4})
	mustCode(t, err, CodeBadConfig)
}

func TestOpenIncompatible(t *testing.T) {
	h, mb, _ := newHAL(t)
	mb.Direct(func(v bus.RegisterView) {
		v.Set(sensor.AddrVersion, sensor.EncodeVersion(sensor.RegVersion{Major: 9, Minor: 0}))
	})
	err := h.Open(context.Background(), sensor.Config{ODR: sensor.ODR100, FSR: sensor.FSR4})
	mustCode(t, err, CodeIncompatible)
}

func TestTimeoutMapped(t *testing.T) {
	h, mb, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR100, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	mb.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: sensor.AddrStatus, Fault: bus.FaultTimeout})
	_, err := h.Read(ctx)
	mustCode(t, err, CodeTimeout)
}

func TestSuspendAccessAndResume(t *testing.T) {
	h, _, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR100, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	s1, err := h.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := h.Suspend(ctx); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if h.State() != sensor.StateSuspended {
		t.Fatalf("state = %s", h.State())
	}

	// 挂起期间读取：立即得到 CodeSuspended，不返回陈旧数据。
	done := make(chan error, 1)
	go func() { _, err := h.Read(ctx); done <- err }()
	select {
	case err := <-done:
		mustCode(t, err, CodeSuspended)
	case <-time.After(time.Second):
		t.Fatal("read during suspend blocked")
	}

	if err := h.Resume(ctx); err != nil {
		t.Fatalf("resume: %v", err)
	}
	s2, err := h.Read(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if !s2.T.After(s1.T) {
		t.Fatal("resumed sample appears stale")
	}
}

func TestSuspendIdempotentAndInFlightAbort(t *testing.T) {
	h, mb, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR100, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	// 状态寄存器每次事务阻塞 40ms 后超时：采样确定处于在途等待状态。
	mb.AddFault(bus.FaultRule{Kind: bus.OpRead, Addr: sensor.AddrStatus, Fault: bus.FaultTimeout, Delay: 40 * time.Millisecond})

	// 在途采样必须被挂起取消，Suspend 不应长时间阻塞。
	rctx, cancel := context.WithCancel(ctx)
	readErr := make(chan error, 1)
	go func() {
		_, err := h.Read(rctx)
		readErr <- err
	}()
	time.Sleep(5 * time.Millisecond) // 确保采样已在途

	start := time.Now()
	if err := h.Suspend(ctx); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if d := time.Since(start); d > 250*time.Millisecond {
		t.Fatalf("suspend blocked for %v while read in flight", d)
	}
	cancel()
	select {
	case err := <-readErr:
		mustCode(t, err, CodeCanceled)
	case <-time.After(time.Second):
		t.Fatal("in-flight read never returned")
	}

	// 再次挂起是幂等无操作；清除故障后恢复设备。
	if err := h.Suspend(ctx); err != nil {
		t.Fatalf("idempotent suspend: %v", err)
	}
	mb.ClearFaults()
	if err := h.Resume(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentSamplesNoRace(t *testing.T) {
	h, _, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}

	const n = 16
	var wg sync.WaitGroup
	results := make(chan error, n)
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			_, err := h.Read(ctx)
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	for err := range results {
		if err != nil {
			t.Fatalf("concurrent read: %v", err)
		}
	}
}

func TestConcurrentSamplesAreOrderedFrames(t *testing.T) {
	// 串行化采样下，每个成功采样对应驱动内一次突发读，序列值应严格递增，
	// 不会出现两个调用读到同一帧（撕裂/重入）。
	h, _, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	const n = 8
	var mu sync.Mutex
	last := int16(0)
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			s, err := h.Read(ctx)
			if err != nil {
				t.Errorf("read: %v", err)
				return
			}
			mu.Lock()
			if s.X <= last && s.Quality == sensor.QOK {
				t.Errorf("non-monotonic frame: last=%d x=%d", last, s.X)
			}
			last = s.X
			mu.Unlock()
		}()
	}
	wg.Wait()
}

func TestStreamLifecycleAndResources(t *testing.T) {
	h, _, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}

	runtime.GC()
	base := runtime.NumGoroutine()

	sctx, cancel := context.WithCancel(ctx)
	defer cancel()
	ch := h.Stream(sctx)

	frames := 0
	var streamErr error
	deadline := time.After(2 * time.Second)
loop:
	for {
		select {
		case item, ok := <-ch:
			if !ok {
				break loop
			}
			if item.Err != nil {
				streamErr = item.Err
				break loop
			}
			frames++
			if frames >= 20 {
				cancel()
			}
		case <-deadline:
			t.Fatal("stream did not produce enough frames in time")
		}
	}
	if streamErr != nil {
		t.Fatalf("stream error: %v", streamErr)
	}
	if frames < 20 {
		t.Fatalf("want >=20 frames, got %d", frames)
	}
	// 通道必须在取消后关闭。
	deadline = time.After(time.Second)
	select {
	case _, ok := <-ch:
		if ok {
			t.Fatal("channel not closed after cancel")
		}
	case <-deadline:
		t.Fatal("channel close blocked")
	}
	// goroutine 数量应回落到基线附近（容忍少量 runtime 波动）。
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	if n := runtime.NumGoroutine(); n > base+1 {
		t.Fatalf("goroutine leak: base=%d now=%d", base, n)
	}
}

func TestStreamStopsOnSuspend(t *testing.T) {
	h, _, _ := newHAL(t)
	ctx := context.Background()
	if err := h.Open(ctx, sensor.Config{ODR: sensor.ODR400, FSR: sensor.FSR4}); err != nil {
		t.Fatal(err)
	}
	ch := h.Stream(ctx)
	// 读到若干帧后挂起：流必须吐出一个终态错误并关闭，而不是空转。
	gotFrames := 0
	var finalErr error
	deadline := time.After(2 * time.Second)
	for gotFrames < 3 {
		select {
		case item := <-ch:
			if item.Err != nil {
				t.Fatalf("unexpected early stream error: %v", item.Err)
			}
			gotFrames++
		case <-deadline:
			t.Fatal("no frames")
		}
	}
	if err := h.Suspend(ctx); err != nil {
		t.Fatal(err)
	}
	select {
	case item, ok := <-ch:
		if !ok {
			t.Fatal("stream should emit terminal error before close")
		}
		finalErr = item.Err
	case <-deadline:
		t.Fatal("stream did not terminate on suspend")
	}
	mustCode(t, finalErr, CodeCanceled)
	_, ok := <-ch
	if ok {
		t.Fatal("stream channel must close after terminal error")
	}
}

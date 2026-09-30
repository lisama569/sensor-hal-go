package sensor

import (
	"context"
	"testing"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
)

// TestSampleTimeoutWhileBusTransactionInFlight 验证失败归因的稳定性：
// 采样等待窗口（SampleTimeout）到期时，若截止点恰好落在一次在途总线
// 事务内部（事务延迟 > 剩余窗口），结论仍必须是"设备在窗口内没准备好"
// （KindTimeout），而不能因为截止点位置（轮询休眠 vs 在途事务）这种
// 纯粹的调度差异变成 KindCanceled。
//
// 同一故障配置重复运行多次，结论必须一致。
func TestSampleTimeoutWhileBusTransactionInFlight(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		r := newRig(t)
		// 不推进虚拟时钟 => 设备永远不就绪。
		if err := r.drv.Init(context.Background(), Config{ODR: ODR100, FSR: FSR4}); err != nil {
			t.Fatal(err)
		}
		// 窗口 15ms；状态寄存器事务阻塞 40ms => 截止点必然落在在途事务中。
		r.drv.opts.SampleTimeout = 15 * time.Millisecond
		r.drv.opts.ReadTimeout = 0 // 让 dctx 成为唯一生效的截止时间
		r.b.AddFault(bus.FaultRule{
			Kind: bus.OpRead, Addr: AddrStatus,
			Fault: bus.FaultTimeout, Delay: 40 * time.Millisecond,
		})

		parent, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_, err := r.drv.Sample(parent)
		cancel()
		se, ok := AsError(err)
		if !ok {
			t.Fatalf("iter %d: want *sensor.Error, got %T %v", iter, err, err)
		}
		if se.Kind != KindTimeout {
			t.Fatalf("iter %d: want KindTimeout (device not ready in window), got %s: %v",
				iter, se.Kind, err)
		}
	}
}

// TestSampleCallerCancelIsCanceled 与上一用例对照：调用方 ctx 提前结束
// （即使同时存在在途超时事务），结论必须是 KindCanceled，两类"等不下去"
// 必须可区分。
func TestSampleCallerCancelIsCanceled(t *testing.T) {
	for iter := 0; iter < 5; iter++ {
		r := newRig(t)
		if err := r.drv.Init(context.Background(), Config{ODR: ODR100, FSR: FSR4}); err != nil {
			t.Fatal(err)
		}
		r.drv.opts.SampleTimeout = time.Second // 窗口充裕，不会先到期
		r.drv.opts.ReadTimeout = 0
		r.b.AddFault(bus.FaultRule{
			Kind: bus.OpRead, Addr: AddrStatus,
			Fault: bus.FaultTimeout, Delay: 400 * time.Millisecond,
		})

		ctx, cancel := context.WithCancel(context.Background())
		go func() {
			time.Sleep(10 * time.Millisecond)
			cancel()
		}()
		_, err := r.drv.Sample(ctx)
		se, ok := AsError(err)
		if !ok || se.Kind != KindCanceled {
			t.Fatalf("iter %d: want KindCanceled, got %v", iter, err)
		}
	}
}

package observe

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/lisama569/sensor-hal-go/hal"
	"github.com/lisama569/sensor-hal-go/sensor"
)

// Sampler 是观测层采集所依赖的最小采样能力（*hal.HAL 天然满足）。
// 测试可注入假实现以确定性驱动采集循环。
type Sampler interface {
	Read(ctx context.Context) (hal.Sample, error)
}

// stateChecker 是 Sampler 可选实现的接口：返回当前电源状态。
// *hal.HAL 实现了它，用于区分"挂起/关电打断在途采样"（HAL 报
// CANCELED，但设备并未发生新的故障）与真正的异常取消。
type stateChecker interface {
	State() sensor.PowerState
}

const (
	defaultSampleCapacity  = 1024
	defaultFailureCapacity = 256
	defaultRetryInterval   = 20 * time.Millisecond
)

// Options 控制观测层容量与采集节奏。容量一经构造即固定，
// 长期运行中内存占用保持平稳，不随运行天数增长。
type Options struct {
	// SampleCapacity 是保留近期采样的最大条数（<=0 用默认 1024）。
	SampleCapacity int
	// FailureCapacity 是保留失败记录的最大条数（<=0 用默认 256）。
	FailureCapacity int
	// RetryInterval 是两次采样尝试之间的休眠间隔（<=0 用默认 20ms）。
	// 等待一律走定时器休眠（非忙等），休眠可被停止 ctx 立即打断。
	RetryInterval time.Duration
	// Clock 用于帧入缓存时间与失败记录时间；nil 用系统墙钟。
	Clock func() time.Time
}

// DefaultOptions 返回默认容量与节奏。
func DefaultOptions() Options {
	return Options{
		SampleCapacity:  defaultSampleCapacity,
		FailureCapacity: defaultFailureCapacity,
		RetryInterval:   defaultRetryInterval,
	}
}

// Failure 是一条采集失败记录，与近期采样一起用于现场复盘。
// Seq 在单个 Observer 生命周期内单调递增（独立于采样序号）。
type Failure struct {
	Seq      int64
	T        time.Time
	Code     hal.Code
	Op       string
	Message  string
	Attempts int
}

// Observer 在采集进程内长期运行：后台连续采样并把成功帧/失败记录
// 写入有界缓存。读取（Recent/Since/Failures/Stats）与采集互不阻塞。
type Observer struct {
	s    Sampler
	buf  *buffer
	wait time.Duration

	wg sync.WaitGroup
}

// New 创建观测层。零值 Options 等价于 DefaultOptions。
func New(s Sampler, opts Options) *Observer {
	if opts.SampleCapacity <= 0 {
		opts.SampleCapacity = defaultSampleCapacity
	}
	if opts.FailureCapacity <= 0 {
		opts.FailureCapacity = defaultFailureCapacity
	}
	if opts.RetryInterval <= 0 {
		opts.RetryInterval = defaultRetryInterval
	}
	return &Observer{
		s:    s,
		buf:  newBuffer(opts.SampleCapacity, opts.FailureCapacity, opts.Clock),
		wait: opts.RetryInterval,
	}
}

// Run 阻塞执行连续采集，直到 ctx 结束（通常来自 Stop）。
//
// 失败处理约定：
//   - 设备在采样窗口内没准备好、被拒绝、挂起/掉电等设备侧失败，
//     归一化记录到失败缓存后休眠一个 RetryInterval 再继续，
//     进程不中断、不空转；
//   - ctx 结束（调用方主动停止本次采集）不记为设备失败，
//     与"设备没准备好"明确分开，Run 直接返回。
func (o *Observer) Run(ctx context.Context) {
	for {
		if err := ctx.Err(); err != nil {
			return
		}
		s, err := o.s.Read(ctx)
		if err == nil {
			o.buf.addSample(s)
			continue
		}
		// 调用方主动结束：安静退出，不算设备失败。Read 返回后必须
		// 再检查一次 ctx：停止可能恰好打断在途采样，此时 HAL 会以
		// CodeCanceled 返回，但语义上仍是调用方结束而非设备问题。
		if ctx.Err() != nil {
			return
		}
		// 挂起/关电会主动取消在途采样，HAL 对那次读报 CodeCanceled。
		// 这不是一次新的设备故障：跳过，下一轮自然遇到并记录
		// SUSPENDED / POWERED_OFF（归因稳定，不因 Suspend 与窗口到期
		// 的先后而变）。真正的异常取消（设备仍 ACTIVE）才记录。
		var de *hal.DeviceError
		if errors.As(err, &de) && de.Code == hal.CodeCanceled {
			if sc, ok := o.s.(stateChecker); ok && sc.State() != sensor.StateActive {
				if !sleepTimer(ctx, o.wait) {
					return
				}
				continue
			}
		}
		o.buf.addFailure(toFailure(err))
		if !sleepTimer(ctx, o.wait) {
			return
		}
	}
}

// Start 在后台 goroutine 中运行采集，返回停止用 ctx 与 cancel。
// 典型用法：ctx, stop := obs.Start(); defer stop(); defer obs.Wait()。
func (o *Observer) Start() (ctx context.Context, cancel context.CancelFunc) {
	ctx, cancel = context.WithCancel(context.Background())
	o.wg.Add(1)
	go func() {
		defer o.wg.Done()
		o.Run(ctx)
	}()
	return ctx, cancel
}

// Wait 等待后台采集 goroutine 退出（停止后调用，确保资源回收）。
func (o *Observer) Wait() { o.wg.Wait() }

// Recent 返回最近至多 n 条采样快照，详见 buffer.Recent。
func (o *Observer) Recent(n int) Batch { return o.buf.Recent(n) }

// Since 返回 cursor 之后的增量采样，详见 buffer.Since。
func (o *Observer) Since(cursor int64, max int) Batch { return o.buf.Since(cursor, max) }

// Failures 返回当前保留的失败记录（老→新）。
func (o *Observer) Failures() []Failure { return o.buf.Failures() }

// Stats 返回容量与累计计数。
func (o *Observer) Stats() Stats { return o.buf.Stats() }

// sleepTimer 在 ctx 上休眠 d（定时器休眠，非忙等）。
// 返回 false 表示 ctx 在休眠期间结束。
func sleepTimer(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// toFailure 把 HAL 归一化错误转为失败记录；非 HAL 错误归为 CodeInternal。
func toFailure(err error) Failure {
	var de *hal.DeviceError
	if errors.As(err, &de) {
		return Failure{
			Code:     de.Code,
			Op:       de.Op,
			Message:  de.Message,
			Attempts: de.Attempts,
		}
	}
	return Failure{Code: hal.CodeInternal, Op: "sample", Message: err.Error()}
}

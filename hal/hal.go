// Package hal 在 sensor 驱动之上提供对外的硬件抽象门面：
// 统一错误码、串行化的电源状态切换与并发安全的采样访问，
// 以及资源占用稳定的长时间连续采样通道。
package hal

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/lisama569/sensor-hal-go/sensor"
)

// Code 是 HAL 对外的归一化错误码。
type Code int

const (
	CodeOK Code = iota
	CodeTimeout
	CodeRejected
	CodeReadbackMismatch
	CodeBadConfig
	CodeIncompatible
	CodeSuspended
	CodePoweredOff
	CodeIO
	CodeCanceled
	CodeInternal
)

func (c Code) String() string {
	switch c {
	case CodeTimeout:
		return "TIMEOUT"
	case CodeRejected:
		return "REJECTED"
	case CodeReadbackMismatch:
		return "READBACK_MISMATCH"
	case CodeBadConfig:
		return "BAD_CONFIG"
	case CodeIncompatible:
		return "INCOMPATIBLE"
	case CodeSuspended:
		return "SUSPENDED"
	case CodePoweredOff:
		return "POWERED_OFF"
	case CodeIO:
		return "IO"
	case CodeCanceled:
		return "CANCELED"
	case CodeInternal:
		return "INTERNAL"
	default:
		return "OK"
	}
}

// DeviceError 是 HAL 所有对外方法返回的错误类型，根因可区分、可解释。
type DeviceError struct {
	Code     Code
	Op       string
	Message  string
	Attempts int
}

func (e *DeviceError) Error() string {
	s := fmt.Sprintf("hal: op=%s code=%s", e.Op, e.Code)
	if e.Attempts > 0 {
		s += fmt.Sprintf(" attempts=%d", e.Attempts)
	}
	if e.Message != "" {
		s += ": " + e.Message
	}
	return s
}

// Sample 是对外采样结果（复用驱动层带时间/质量标记的结构）。
type Sample = sensor.Sample

// Config 是对外配置。
type Config = sensor.Config

// StreamItem 是连续采样通道上的一条消息（Sample 与 Err 互斥出现）。
type StreamItem struct {
	Sample Sample
	Err    error
}

// HAL 是单设备硬件抽象层。
//
// 并发模型：
//   - pmu 串行化电源状态切换；
//   - sampleGate 是容量为 1 的令牌，保证同一时刻只有一个采样事务在途，
//     获取令牌可被 ctx 取消，因此 Suspend 不会被在途采样无限阻塞；
//   - activeCancel 用于让 Suspend 主动取消在途采样，使挂起立即生效；
//   - 采样路径不持有自旋锁轮询硬件，等待数据就绪时休眠，不存在忙等。
type HAL struct {
	drv *sensor.Driver

	pmu        sync.Mutex
	sampleGate chan struct{}

	mu           sync.Mutex
	activeCancel context.CancelFunc
}

// New 基于已构造的驱动创建 HAL。
func New(drv *sensor.Driver) *HAL {
	return &HAL{
		drv:        drv,
		sampleGate: make(chan struct{}, 1),
	}
}

// acquireSample 获取在途采样令牌；ctx 取消时立即返回 CodeCanceled。
func (h *HAL) acquireSample(ctx context.Context) error {
	select {
	case h.sampleGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return mapErr("sample", ctx.Err())
	}
}

func (h *HAL) releaseSample() { <-h.sampleGate }

// registerActive 登记一个可被电源切换取消的采样上下文，返回其 cancel。
func (h *HAL) registerActive() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	h.mu.Lock()
	h.activeCancel = cancel
	h.mu.Unlock()
	return ctx, cancel
}

// clearActive 注销（仅当登记的仍是自己）并取消上下文。
func (h *HAL) clearActive(cancel context.CancelFunc) {
	h.mu.Lock()
	if h.activeCancel != nil {
		// 无法比较函数指针身份时也只需清空：后来者会覆盖此字段。
		h.activeCancel = nil
	}
	h.mu.Unlock()
	cancel()
}

// abortActive 取消在途采样（若有）。
func (h *HAL) abortActive() {
	h.mu.Lock()
	c := h.activeCancel
	h.activeCancel = nil
	h.mu.Unlock()
	if c != nil {
		c()
	}
}

// Open 完成探测、版本兼容判定与初始化。
func (h *HAL) Open(ctx context.Context, cfg Config) error {
	h.pmu.Lock()
	defer h.pmu.Unlock()
	return mapErr("open", h.drv.Init(ctx, cfg))
}

// Read 执行一次采样。挂起/掉电等状态下由驱动立即返回对应错误码，
// 不返回陈旧数据；并发调用被串行化且可被 ctx 取消。
func (h *HAL) Read(ctx context.Context) (Sample, error) {
	if err := h.acquireSample(ctx); err != nil {
		return Sample{}, err
	}
	defer h.releaseSample()

	active, activeCancel := h.registerActive()
	defer h.clearActive(activeCancel)

	// 驱动采样在 (调用方 ctx ∪ 电源切换取消) 任一触发时结束。
	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-active.Done():
			cancel()
		case <-cctx.Done():
		}
	}()

	s, err := h.drv.Sample(cctx)
	if err != nil {
		return Sample{}, mapErr("sample", err)
	}
	return s, nil
}

// Suspend 挂起设备（已挂起时为幂等无操作）。在途采样会被取消，
// 因此挂起快速生效；挂起后新操作得到 CodeSuspended。
func (h *HAL) Suspend(ctx context.Context) error {
	h.pmu.Lock()
	defer h.pmu.Unlock()
	h.abortActive()
	if h.drv.State() == sensor.StateSuspended {
		return nil
	}
	return mapErr("suspend", h.drv.Suspend(ctx))
}

// Resume 恢复设备；若掉电丢配置，由驱动层重新初始化。
func (h *HAL) Resume(ctx context.Context) error {
	h.pmu.Lock()
	defer h.pmu.Unlock()
	return mapErr("resume", h.drv.Resume(ctx))
}

// Shutdown 完全关闭设备并取消在途采样。
func (h *HAL) Shutdown(ctx context.Context) error {
	h.pmu.Lock()
	defer h.pmu.Unlock()
	h.abortActive()
	return mapErr("shutdown", h.drv.PowerOff(ctx))
}

// State 返回当前电源状态。
func (h *HAL) State() sensor.PowerState { return h.drv.State() }

// Stream 启动长时间连续采样，返回无缓冲只读通道：每帧 Sample 后可能跟随
// 一条终态 Err，随后通道关闭。
//
// 资源稳定性：只启动一个 goroutine；它与 Read 共用 sampleGate，随取随还，
// 不累积 goroutine/缓冲；ctx 取消后该 goroutine 必然退出并关闭通道。
func (h *HAL) Stream(ctx context.Context) <-chan StreamItem {
	out := make(chan StreamItem)
	go func() {
		defer close(out)
		for {
			// 复用 Read：帧级串行、可被挂起取消，保证长时间运行也只有
			// 本 goroutine + 每帧一个短暂的取消桥接 goroutine（随帧退出）。
			s, err := h.Read(ctx)
			if err != nil {
				// 调用方取消是正常结束，不作为设备错误下发。
				if ctx.Err() != nil {
					return
				}
				select {
				case out <- StreamItem{Err: err}:
				case <-ctx.Done():
					return
				}
				// 挂起/掉电/不兼容等终态不再空转：交由调用方恢复后重新建流。
				return
			}
			select {
			case out <- StreamItem{Sample: s}:
			case <-ctx.Done():
				return
			}
		}
	}()
	return out
}

// mapErr 把驱动错误归一化为 HAL 错误码。
func mapErr(op string, err error) error {
	if err == nil {
		return nil
	}
	var se *sensor.Error
	if errors.As(err, &se) {
		de := &DeviceError{Op: op, Message: se.Detail, Attempts: se.Attempts}
		switch se.Kind {
		case sensor.KindTimeout:
			de.Code = CodeTimeout
		case sensor.KindRejected:
			de.Code = CodeRejected
		case sensor.KindReadbackMismatch:
			de.Code = CodeReadbackMismatch
		case sensor.KindBadConfig:
			de.Code = CodeBadConfig
		case sensor.KindIncompatible:
			de.Code = CodeIncompatible
		case sensor.KindSuspended:
			de.Code = CodeSuspended
		case sensor.KindPoweredOff:
			de.Code = CodePoweredOff
		case sensor.KindCanceled:
			de.Code = CodeCanceled
		case sensor.KindIO:
			de.Code = CodeIO
		default:
			de.Code = CodeInternal
		}
		return de
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return &DeviceError{Code: CodeCanceled, Op: op, Message: err.Error()}
	}
	return &DeviceError{Code: CodeInternal, Op: op, Message: err.Error()}
}

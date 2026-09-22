package sensor

import (
	"context"
	"errors"
	"fmt"

	"github.com/lisama569/sensor-hal-go/bus"
)

// Kind 是对外归一化后的错误类别。不同调用路径（探测 / 初始化 / 采样 /
// 电源切换）遇到同类根因时必须映射到同一个 Kind，保证失败原因可区分、可解释。
type Kind int

const (
	KindNone             Kind = iota
	KindTimeout               // 总线超时：从机未在限定时间内应答
	KindRejected              // 设备拒绝：设备明确拒绝该事务（如掉电状态下写）
	KindReadbackMismatch      // 回读不一致：写入后的读回值与期望不符
	KindBadConfig             // 配置非法：参数超出设备允许范围
	KindIncompatible          // 版本不兼容：寄存器映射版本不被本驱动接受
	KindSuspended             // 挂起中访问：设备处于 SUSPENDED，操作不执行
	KindPoweredOff            // 掉电：设备处于 OFF，必须重新初始化
	KindIO                    // 其他总线/IO 错误
	KindCanceled              // 调用方取消 / 截止时间到期
	KindInternal              // 驱动内部错误（不应发生）
)

func (k Kind) String() string {
	switch k {
	case KindTimeout:
		return "TIMEOUT"
	case KindRejected:
		return "REJECTED"
	case KindReadbackMismatch:
		return "READBACK_MISMATCH"
	case KindBadConfig:
		return "BAD_CONFIG"
	case KindIncompatible:
		return "INCOMPATIBLE"
	case KindSuspended:
		return "SUSPENDED"
	case KindPoweredOff:
		return "POWERED_OFF"
	case KindIO:
		return "IO"
	case KindCanceled:
		return "CANCELED"
	case KindInternal:
		return "INTERNAL"
	default:
		return "NONE"
	}
}

// Error 是传感器驱动对外暴露的统一错误类型。
//
// 失败终态的错误 Retryable 恒为 false：调用方不应自行重试，驱动内部
// 的有限重试（仅读操作）也只在仍有机会时发生，一旦返回即表示重试已耗尽，
// 不会掩盖失败；写操作永不重试，因此不存在重复副作用。
type Error struct {
	Kind      Kind   // 归一化类别
	Op        string // 发生错误的操作（probe/init/sample/...）
	Detail    string // 人类可读细节
	Attempts  int    // 已尝试次数（含首次）
	Retryable bool   // 是否仍可重试（终态失败恒为 false）
}

func (e *Error) Error() string {
	if e == nil {
		return "<nil>"
	}
	s := fmt.Sprintf("sensor: op=%s kind=%s", e.Op, e.Kind)
	if e.Attempts > 0 {
		s += fmt.Sprintf(" attempts=%d", e.Attempts)
	}
	if e.Detail != "" {
		s += ": " + e.Detail
	}
	return s
}

// AsError 从 error 链中提取 *Error。
func AsError(err error) (*Error, bool) {
	var e *Error
	if errors.As(err, &e) {
		return e, true
	}
	return nil, false
}

// fail 构造一个终态（不可重试）的驱动错误。
func fail(kind Kind, op, detail string, attempts int) *Error {
	return &Error{Kind: kind, Op: op, Detail: detail, Attempts: attempts, Retryable: false}
}

// classify 把底层错误归一化为 Kind。
func classify(err error) Kind {
	switch {
	case err == nil:
		return KindNone
	case errors.Is(err, bus.ErrTimeout):
		return KindTimeout
	case errors.Is(err, bus.ErrRejected):
		return KindRejected
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		return KindCanceled
	default:
		return KindIO
	}
}

// Package sensor 实现一个内存总线之上的加速度传感器驱动：
// 设备探测、初始化、采样、寄存器映射版本兼容判定与电源状态机。
//
// 关键约定：
//   - 只有读事务允许有限重试（且仅对总线超时重试）；写事务一次定音，
//     因此重试不会产生重复副作用，也不会用"成功的重试"掩盖已发生的失败。
//   - 每次写寄存器后立即回读校验，回读不一致作为独立错误类别上报。
//   - 所有方法在单把互斥锁下推进状态机；挂起期间的操作立即失败，
//     不发起总线事务、不返回陈旧数据。
package sensor

import (
	"context"
	"sync"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
)

// 寄存器地址映射（设备固定布局；Minor 升级只能向后追加）。
const (
	AddrChipID  uint8 = 0x00
	AddrVersion uint8 = 0x01
	AddrStatus  uint8 = 0x02
	AddrCtrl    uint8 = 0x03
	AddrODR     uint8 = 0x04
	AddrFSR     uint8 = 0x05
	AddrDataX   uint8 = 0x10
	AddrDataY   uint8 = 0x11
	AddrDataZ   uint8 = 0x12
	AddrConfSig uint8 = 0x1F // 配置签名：掉电丢失配置时归零
)

// 配置签名魔数：初始化完成时写入；恢复时据此判断配置是否丢失。
const configSignature uint16 = 0xC0FE

const (
	// ChipIDA 是本驱动支持的芯片 ID。
	ChipIDA uint16 = 0xA1
)

// 状态寄存器位。
const (
	StatusDRDY uint16 = 1 << 0 // 数据就绪
	StatusOVR  uint16 = 1 << 1 // 数据覆盖（采样质量下降）
	StatusRCAL uint16 = 1 << 2 // 需要重新校准
)

// 控制寄存器位（电源状态）。
const (
	CtrlPowerOn uint16 = 0x0001
	CtrlSuspend uint16 = 0x0002
)

// Quality 标记单个采样的质量。
type Quality int

const (
	QOK         Quality = iota // 正常
	QOverrun                   // 发生数据覆盖：未及时取走上一帧
	QNeedsRecal                // 设备提示需重新校准
)

func (q Quality) String() string {
	switch q {
	case QOverrun:
		return "OVERRUN"
	case QNeedsRecal:
		return "NEEDS_RECAL"
	default:
		return "OK"
	}
}

// Sample 是一次采样结果，带明确时间与质量标记。
type Sample struct {
	T       time.Time
	X, Y, Z int16
	Quality Quality
	ODR     Hz
	FSR     G
}

// PowerState 表示设备电源状态。
type PowerState int

const (
	StateOff PowerState = iota
	StateActive
	StateSuspended
)

func (s PowerState) String() string {
	switch s {
	case StateActive:
		return "ACTIVE"
	case StateSuspended:
		return "SUSPENDED"
	default:
		return "OFF"
	}
}

// Options 控制驱动的重试、超时与轮询行为。
type Options struct {
	ReadRetry     int           // 读事务最大额外重试次数（仅对超时生效）
	ReadTimeout   time.Duration // 单次读事务超时（<=0 表示不另设超时）
	WriteTimeout  time.Duration // 单次写事务超时（<=0 表示不另设超时）
	SampleTimeout time.Duration // 等待数据就绪的上限
	PollInterval  time.Duration // 数据就绪轮询间隔（休眠等待，非忙等）
	Clock         func() time.Time
}

// DefaultOptions 返回默认参数。
func DefaultOptions() Options {
	return Options{
		ReadRetry:     2,
		ReadTimeout:   50 * time.Millisecond,
		WriteTimeout:  50 * time.Millisecond,
		SampleTimeout: 200 * time.Millisecond,
		PollInterval:  time.Millisecond,
	}
}

// nopLogger 在未提供 Logger 时使用。
type nopLogger struct{}

func (nopLogger) Printf(string, ...any) {}

// Logger 用于打印读写激励与状态迁移（单测可接 testing.T）。
type Logger interface {
	Printf(format string, args ...any)
}

// Driver 是传感器设备驱动。所有方法均并发安全。
type Driver struct {
	b             bus.Bus
	lg            Logger
	opts          Options
	driverVersion RegVersion
	knownChips    map[uint16]bool
	now           func() time.Time

	mu       sync.Mutex
	state    PowerState
	cfg      Config
	cfgSaved bool
	inited   bool
	compat   Compatibility
}

// New 创建驱动。version 为驱动编译时支持的寄存器映射版本。
func New(b bus.Bus, version RegVersion, lg Logger, opts Options) *Driver {
	if lg == nil {
		lg = nopLogger{}
	}
	if opts.Clock == nil {
		opts.Clock = time.Now
	}
	return &Driver{
		b:             b,
		lg:            lg,
		opts:          opts,
		driverVersion: version,
		knownChips:    map[uint16]bool{ChipIDA: true},
		now:           opts.Clock,
		state:         StateOff,
	}
}

// State 返回当前电源状态。
func (d *Driver) State() PowerState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state
}

func (d *Driver) transition(op string, from, to PowerState) {
	if from == to {
		return
	}
	d.lg.Printf("STATE %s: %s -> %s", op, from, to)
	d.state = to
}

// withTimeout 按超时时长派生子 ctx；timeout<=0 时直接使用父 ctx。
func withTimeout(parent context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	if timeout <= 0 {
		return parent, func() {}
	}
	return context.WithTimeout(parent, timeout)
}

// readOnce 执行一次读事务并把错误归一化。
func (d *Driver) readOnce(ctx context.Context, op string, addr uint8, timeout time.Duration) (uint16, *Error) {
	rctx, cancel := withTimeout(ctx, timeout)
	defer cancel()
	v, err := d.b.Read(rctx, addr)
	if err != nil {
		return 0, fail(classify(err), op, err.Error(), 1)
	}
	return v, nil
}

// readRetry 执行读事务：仅在总线超时时按 opts.ReadRetry 重试，
// 其余错误（拒绝/取消等）立即返回。返回的 *Error.Attempts 记录实际尝试次数。
// 外层 ctx 被取消（例如挂起取消在途采样）时，错误类别为 KindCanceled，
// 不与总线超时混淆。
func (d *Driver) readRetry(ctx context.Context, op string, addr uint8) (uint16, *Error) {
	var last *Error
	for attempt := 1; attempt <= d.opts.ReadRetry+1; attempt++ {
		// 在发起尝试前先响应取消，避免取消与最后一次尝试竞争。
		if err := ctx.Err(); err != nil {
			if last != nil {
				return 0, d.adjustCanceled(ctx, last)
			}
			return 0, fail(KindCanceled, op, err.Error(), attempt-1)
		}
		v, e := d.readOnce(ctx, op, addr, d.opts.ReadTimeout)
		if e == nil {
			if attempt > 1 {
				d.lg.Printf("READ 0x%02X ok on attempt %d", addr, attempt)
			}
			return v, nil
		}
		e.Attempts = attempt
		last = e
		if e.Kind != KindTimeout || attempt > d.opts.ReadRetry {
			return 0, d.adjustCanceled(ctx, e)
		}
		d.lg.Printf("READ 0x%02X timeout (attempt %d/%d), retrying", addr, attempt, d.opts.ReadRetry+1)
	}
	return 0, d.adjustCanceled(ctx, last)
}

// adjustCanceled 在外层 ctx 已结束时把错误改写为 KindCanceled。
func (d *Driver) adjustCanceled(ctx context.Context, e *Error) *Error {
	if ctx.Err() != nil && e.Kind != KindCanceled {
		ne := *e
		ne.Kind = KindCanceled
		ne.Detail = "canceled while " + e.Op + ": " + ctx.Err().Error()
		return &ne
	}
	return e
}

// burstRetry 与 readRetry 语义一致，用于多寄存器突发读。
func (d *Driver) burstRetry(ctx context.Context, op string, addr uint8, n int) ([]uint16, *Error) {
	var last *Error
	for attempt := 1; attempt <= d.opts.ReadRetry+1; attempt++ {
		rctx, cancel := withTimeout(ctx, d.opts.ReadTimeout)
		vals, err := d.b.ReadBurst(rctx, addr, n)
		cancel()
		if err == nil {
			return vals, nil
		}
		e := fail(classify(err), op, err.Error(), attempt)
		last = e
		if e.Kind != KindTimeout || attempt > d.opts.ReadRetry {
			return nil, d.adjustCanceled(ctx, e)
		}
		d.lg.Printf("BURST 0x%02X n=%d timeout (attempt %d/%d), retrying", addr, n, attempt, d.opts.ReadRetry+1)
	}
	return nil, d.adjustCanceled(ctx, last)
}

// writeVerified 执行一次写并立即回读校验。写永不重试；
// 回读值不符返回 KindReadbackMismatch（与超时、拒绝明确区分）。
func (d *Driver) writeVerified(ctx context.Context, op string, addr uint8, value uint16) *Error {
	wctx, cancel := withTimeout(ctx, d.opts.WriteTimeout)
	err := d.b.Write(wctx, addr, value)
	cancel()
	if err != nil {
		return fail(classify(err), op, err.Error(), 1)
	}
	got, e := d.readRetry(ctx, op, addr)
	if e != nil {
		e.Detail = "readback failed: " + e.Detail
		return e
	}
	if got != value {
		return fail(KindReadbackMismatch, op,
			sprintfReadback(addr, value, got), 1)
	}
	return nil
}

func sprintfReadback(addr uint8, want, got uint16) string {
	return "readback mismatch at 0x" + hex8(addr) + ": want=0x" + hex16(want) + " got=0x" + hex16(got)
}

func hex8(v uint8) string {
	const h = "0123456789ABCDEF"
	return string([]byte{h[v>>4], h[v&0x0F]})
}

func hex16(v uint16) string {
	return hex8(uint8(v>>8)) + hex8(uint8(v))
}

// Probe 探测设备：校验芯片 ID 与寄存器映射版本兼容性。
// 挂起状态下立即失败，不发起任何总线事务。
func (d *Driver) Probe(ctx context.Context) (Compatibility, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == StateSuspended {
		return Compatibility{}, fail(KindSuspended, "probe", "device suspended; resume before probe", 0)
	}
	c, e := d.probeLocked(ctx)
	if e != nil {
		return c, e
	}
	d.compat = c
	return c, nil
}

// configureLocked 在状态机锁内写入配置并回读确认（调用方持锁）。
func (d *Driver) configureLocked(ctx context.Context, cfg Config) *Error {
	if e := d.writeVerified(ctx, "init", AddrCtrl, CtrlPowerOn); e != nil {
		return e
	}
	if e := d.writeVerified(ctx, "init", AddrODR, encODR(cfg.ODR)); e != nil {
		return e
	}
	if e := d.writeVerified(ctx, "init", AddrFSR, encFSR(cfg.FSR)); e != nil {
		return e
	}
	if e := d.writeVerified(ctx, "init", AddrConfSig, configSignature); e != nil {
		return e
	}
	d.cfg = cfg
	d.cfgSaved = true
	return nil
}

// Init 探测并初始化设备，写入配置并以回读确认。
// 非法配置在任何总线事务之前被拒绝。
func (d *Driver) Init(ctx context.Context, cfg Config) error {
	if err := cfg.Validate(); err != nil {
		return err
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == StateSuspended {
		return fail(KindSuspended, "init", "device suspended; resume before init", 0)
	}
	return d.initLocked(ctx, cfg)
}

// initLocked 执行探测与配置（调用方持 d.mu）。
func (d *Driver) initLocked(ctx context.Context, cfg Config) error {
	if c, e := d.probeLocked(ctx); e != nil {
		return e
	} else {
		d.compat = c
	}
	before := d.state
	if e := d.configureLocked(ctx, cfg); e != nil {
		return e
	}
	d.transition("init", before, StateActive)
	return nil
}

// probeLocked 是不加锁的探测实现（调用方持 d.mu）。
func (d *Driver) probeLocked(ctx context.Context) (Compatibility, *Error) {
	id, e := d.readRetry(ctx, "probe", AddrChipID)
	if e != nil {
		return Compatibility{}, e
	}
	raw, e := d.readRetry(ctx, "probe", AddrVersion)
	if e != nil {
		return Compatibility{}, e
	}
	c := CheckCompatibility(id, raw, d.driverVersion, d.knownChips)
	d.lg.Printf("PROBE chip=0x%04X device=%s driver=%s ok=%t reason=%s",
		id, c.Device, c.Driver, c.OK, c.Reason)
	if !c.OK {
		return c, fail(KindIncompatible, "probe", c.Message, 1)
	}
	return c, nil
}

// Sample 执行一次采样：休眠轮询数据就绪位，然后突发读取三轴。
// 仅 ACTIVE 状态可采样；挂起中立即返回 KindSuspended，掉电返回
// KindPoweredOff，均不触碰总线，绝不返回陈旧数据。
func (d *Driver) Sample(ctx context.Context) (Sample, error) {
	d.mu.Lock()
	switch d.state {
	case StateSuspended:
		d.mu.Unlock()
		return Sample{}, fail(KindSuspended, "sample", "device suspended", 0)
	case StateOff:
		d.mu.Unlock()
		return Sample{}, fail(KindPoweredOff, "sample", "device powered off; init required", 0)
	}
	cfg := d.cfg
	d.mu.Unlock()

	deadline := time.Now().Add(d.opts.SampleTimeout)
	dctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()

	for {
		status, e := d.readRetry(dctx, "sample", AddrStatus)
		if e != nil {
			return Sample{}, d.sampleWindowErr(ctx, e)
		}
		if status&StatusDRDY != 0 {
			q := QOK
			if status&StatusOVR != 0 {
				q = QOverrun
			}
			if status&StatusRCAL != 0 && q == QOK {
				q = QNeedsRecal
			}
			values, e := d.burstRetry(dctx, "sample", AddrDataX, 3)
			if e != nil {
				return Sample{}, d.sampleWindowErr(ctx, e)
			}
			s := Sample{
				T:       d.now(),
				X:       int16(values[0]),
				Y:       int16(values[1]),
				Z:       int16(values[2]),
				Quality: q,
				ODR:     cfg.ODR,
				FSR:     cfg.FSR,
			}
			d.lg.Printf("SAMPLE t=%s x=%d y=%d z=%d q=%s", s.T.Format(time.RFC3339Nano), s.X, s.Y, s.Z, q)
			return s, nil
		}

		if err := sleepWait(dctx, d.opts.PollInterval); err != nil {
			if ctx.Err() != nil {
				return Sample{}, fail(KindCanceled, "sample", ctx.Err().Error(), 0)
			}
			return Sample{}, fail(KindTimeout, "sample", "data not ready within sample timeout", 0)
		}
	}
}

// sleepWait 在 ctx 上休眠等待 d（非忙等）。
func sleepWait(ctx context.Context, d time.Duration) error {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// sampleWindowErr 归一化采样就绪窗口内读事务的终态错误：
//   - 外层调用方 ctx 已结束（主动取消 / 调用方自己的截止）=> KindCanceled；
//   - 否则只要是内部采样截止窗口到点（即便截止恰好打断一次读事务，
//     readRetry 会把 DeadlineExceeded 报成 KindCanceled），一律归因为
//     KindTimeout（设备在窗口内一直未就绪）。
//
// 这样同一设备行为下结论不随截止时间与总线事务的调度先后而变化。
func (d *Driver) sampleWindowErr(caller context.Context, e *Error) *Error {
	if caller.Err() != nil {
		if e.Kind == KindCanceled {
			return e
		}
		ne := *e
		ne.Kind = KindCanceled
		ne.Detail = "canceled while sample: " + caller.Err().Error()
		return &ne
	}
	if e.Kind == KindCanceled {
		ne := *e
		ne.Kind = KindTimeout
		ne.Detail = "data not ready within sample timeout"
		return &ne
	}
	return e
}

// Suspend 挂起设备：写挂起位并保留配置。ACTIVE 以外状态调用立即报错。
func (d *Driver) Suspend(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == StateSuspended {
		return fail(KindSuspended, "suspend", "already suspended", 0)
	}
	if d.state == StateOff {
		return fail(KindPoweredOff, "suspend", "device powered off; init required", 0)
	}
	if e := d.writeVerified(ctx, "suspend", AddrCtrl, CtrlPowerOn|CtrlSuspend); e != nil {
		return e
	}
	d.transition("suspend", StateActive, StateSuspended)
	return nil
}

// Resume 从挂起恢复。若掉电导致配置签名丢失，则按保存的配置完整重新
// 初始化，而不是沿用驱动内缓存的旧状态。
func (d *Driver) Resume(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state != StateSuspended {
		return fail(KindIO, "resume", "resume called in state "+d.state.String(), 0)
	}

	if e := d.writeVerified(ctx, "resume", AddrCtrl, CtrlPowerOn); e != nil {
		return e
	}
	sig, e := d.readRetry(ctx, "resume", AddrConfSig)
	if e != nil {
		return e
	}
	if sig != configSignature || !d.cfgSaved {
		d.lg.Printf("RESUME config lost (sig=0x%04X), re-initializing", sig)
		if e := d.configureLocked(ctx, d.cfg); e != nil {
			return e
		}
	} else {
		d.cfgSaved = true
	}
	d.transition("resume", StateSuspended, StateActive)
	return nil
}

// PowerOff 完全掉电。掉电后只有重新 Init 才能使用设备。
func (d *Driver) PowerOff(ctx context.Context) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.state == StateOff {
		return nil // 幂等
	}
	before := d.state
	if e := d.writeVerified(ctx, "poweroff", AddrCtrl, 0); e != nil {
		return e
	}
	d.cfgSaved = false
	d.transition("poweroff", before, StateOff)
	return nil
}

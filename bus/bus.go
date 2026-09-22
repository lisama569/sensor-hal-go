// Package bus 提供以内存模拟的传感器寄存器总线。
//
// 总线以 16 位寄存器为最小寻址单元，支持单寄存器读写与多寄存器突发读，
// 并可通过 FaultRule 注入确定性的总线级故障（超时 / 拒绝 / 数据篡改），
// 供上层驱动在不依赖真实硬件与外部服务的情况下验证错误路径。
//
// 所有事务在单把互斥锁下串行完成，突发读在同一临界区内取数，
// 因此总线上不可能出现跨事务的撕裂读。
package bus

import (
	"context"
	"errors"
	"sync"
	"time"
)

// 总线级哨兵错误，供上层驱动做归一化分类。
var (
	// ErrTimeout 表示从机未在限定时间内应答（总线超时）。
	ErrTimeout = errors.New("bus: transaction timeout")
	// ErrRejected 表示设备明确拒绝该事务（例如掉电状态下拒绝写入）。
	ErrRejected = errors.New("bus: transaction rejected by device")
)

// OpKind 标识一次总线事务的类型。
type OpKind int

const (
	OpRead  OpKind = iota // 读单个寄存器
	OpWrite               // 写单个寄存器
	OpBurst               // 突发读多个连续寄存器
)

func (k OpKind) String() string {
	switch k {
	case OpRead:
		return "READ"
	case OpWrite:
		return "WRITE"
	case OpBurst:
		return "BURST"
	default:
		return "UNKNOWN"
	}
}

// FaultKind 定义总线级故障类别。
type FaultKind int

const (
	FaultNone    FaultKind = iota // 正常
	FaultTimeout                  // 总线超时（从机无应答）
	FaultReject                   // 设备拒绝
	FaultCorrupt                  // 回读/返回数据被篡改
)

func (k FaultKind) String() string {
	switch k {
	case FaultTimeout:
		return "TIMEOUT"
	case FaultReject:
		return "REJECT"
	case FaultCorrupt:
		return "CORRUPT"
	default:
		return "NONE"
	}
}

// LogEntry 记录一次总线读写激励，便于测试打印与断言。
type LogEntry struct {
	Seq       int           // 全局递增序号
	Kind      OpKind        // 事务类型
	Addr      uint8         // 起始寄存器地址
	Values    []uint16      // 写入值或读出值
	Wait      time.Duration // 本次事务耗时
	Fault     FaultKind     // 注入/发生的故障
	Timestamp time.Time     // 发生时刻
}

// Bus 是寄存器总线的抽象，真实平台可替换为 I2C/SPI 实现。
type Bus interface {
	Read(ctx context.Context, addr uint8) (uint16, error)
	Write(ctx context.Context, addr uint8, value uint16) error
	ReadBurst(ctx context.Context, addr uint8, n int) ([]uint16, error)
	Log() []LogEntry
}

// FaultRule 描述一条故障注入规则：在指定事务发生时注入指定故障。
// Times 为 0 表示持续生效；>0 表示仅前 Times 次命中生效。
type FaultRule struct {
	Kind  OpKind
	Addr  uint8
	Times int // 命中次数上限；0 表示持续命中
	Fault FaultKind
	Delay time.Duration // 仅 FaultTimeout 使用：模拟从机应答延迟

	hitCount int
}

// RegisterView 是事务临界区内提供给设备模型的寄存器视图。
// 视图方法仅允许在 Model 回调内使用。
type RegisterView struct {
	regs map[uint8]uint16
}

// Get 读取一个寄存器（未映射地址返回 0）。
func (v RegisterView) Get(addr uint8) uint16 { return v.regs[addr] }

// Set 写入一个寄存器。
func (v RegisterView) Set(addr uint8, value uint16) { v.regs[addr] = value }

// Model 模拟挂接在总线上的设备行为（如状态机推进、数据生成）。
// 回调在总线锁内执行，实现方不得阻塞或再次访问总线。
type Model interface {
	OnWrite(v RegisterView, addr uint8, value uint16, now time.Time)
	OnRead(v RegisterView, addr uint8, now time.Time)
	OnBurst(v RegisterView, addr uint8, n int, now time.Time)
}

// WriteGate 是 Model 可选实现的接口：返回 false 时设备在寄存器更新前
// 拒绝该写事务（总线返回 ErrRejected，寄存器内容不变、OnWrite 不触发）。
type WriteGate interface {
	AllowWrite(v RegisterView, addr uint8, value uint16, now time.Time) bool
}

// MemBus 是基于内存映射寄存器的确定性总线模拟。
type MemBus struct {
	mu      sync.Mutex
	regs    map[uint8]uint16
	clk     func() time.Time
	log     []LogEntry
	seq     int
	rules   []*FaultRule
	latency time.Duration // 每次事务的基础耗时
	model   Model
}

// NewMemBus 创建内存总线。regs 为初始寄存器内容（会被拷贝）；
// clk 为 nil 时使用系统墙钟。
func NewMemBus(regs map[uint8]uint16, clk func() time.Time) *MemBus {
	cp := make(map[uint8]uint16, len(regs))
	for k, v := range regs {
		cp[k] = v
	}
	if clk == nil {
		clk = time.Now
	}
	return &MemBus{regs: cp, clk: clk}
}

// SetLatency 设置每次总线事务的基础耗时（默认 0）。
func (b *MemBus) SetLatency(d time.Duration) {
	b.mu.Lock()
	b.latency = d
	b.mu.Unlock()
}

// SetModel 挂接设备行为模型。
func (b *MemBus) SetModel(m Model) {
	b.mu.Lock()
	b.model = m
	b.mu.Unlock()
}

// AddFault 安装一条故障注入规则（后安装的规则优先匹配）。
func (b *MemBus) AddFault(rule FaultRule) {
	b.mu.Lock()
	b.rules = append(b.rules, &rule)
	b.mu.Unlock()
}

// ClearFaults 清除全部故障规则。
func (b *MemBus) ClearFaults() {
	b.mu.Lock()
	b.rules = nil
	b.mu.Unlock()
}

// Snapshot 返回当前寄存器内容的拷贝（测试断言用）。
func (b *MemBus) Snapshot() map[uint8]uint16 {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make(map[uint8]uint16, len(b.regs))
	for k, v := range b.regs {
		cp[k] = v
	}
	return cp
}

// Direct 在总线锁内执行 fn，供设备模型/测试确定性地修改寄存器状态
// （例如模拟突发掉电导致配置寄存器复位）。不产生事务日志。
func (b *MemBus) Direct(fn func(v RegisterView)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	fn(RegisterView{regs: b.regs})
}

// Log 返回截至目前的全部总线事务日志（拷贝）。
func (b *MemBus) Log() []LogEntry {
	b.mu.Lock()
	defer b.mu.Unlock()
	cp := make([]LogEntry, len(b.log))
	copy(cp, b.log)
	return cp
}

// matchFault 在锁内查找当前事务命中的故障规则。
func (b *MemBus) matchFault(kind OpKind, addr uint8) *FaultRule {
	for i := len(b.rules) - 1; i >= 0; i-- {
		r := b.rules[i]
		if r.Kind != kind || r.Addr != addr {
			continue
		}
		if r.Times > 0 && r.hitCount >= r.Times {
			continue
		}
		r.hitCount++
		return r
	}
	return nil
}

// record 在锁内追加一条事务日志。
func (b *MemBus) record(start time.Time, kind OpKind, addr uint8, vals []uint16, f FaultKind) {
	b.seq++
	b.log = append(b.log, LogEntry{
		Seq:       b.seq,
		Kind:      kind,
		Addr:      addr,
		Values:    append([]uint16(nil), vals...),
		Wait:      b.clk().Sub(start),
		Fault:     f,
		Timestamp: start,
	})
}

// sleepCtx 等待 d 或 ctx 结束；返回 ctx.Err()（超时/取消）或 nil。
func sleepCtx(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (b *MemBus) Read(ctx context.Context, addr uint8) (uint16, error) {
	start := b.clk()
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := sleepCtx(ctx, b.latency); err != nil {
		b.record(start, OpRead, addr, nil, FaultNone)
		return 0, err
	}

	v := RegisterView{regs: b.regs}
	if b.model != nil {
		b.model.OnRead(v, addr, b.clk())
	}
	value := b.regs[addr]

	if r := b.matchFault(OpRead, addr); r != nil {
		switch r.Fault {
		case FaultTimeout:
			b.record(start, OpRead, addr, nil, FaultTimeout)
			if err := sleepCtx(ctx, r.Delay); err != nil {
				return 0, err
			}
			return 0, ErrTimeout
		case FaultReject:
			b.record(start, OpRead, addr, nil, FaultReject)
			return 0, ErrRejected
		case FaultCorrupt:
			value = ^value
			b.record(start, OpRead, addr, []uint16{value}, FaultCorrupt)
			return value, nil
		}
	}

	b.record(start, OpRead, addr, []uint16{value}, FaultNone)
	return value, nil
}

func (b *MemBus) Write(ctx context.Context, addr uint8, value uint16) error {
	start := b.clk()
	b.mu.Lock()
	defer b.mu.Unlock()

	if err := sleepCtx(ctx, b.latency); err != nil {
		b.record(start, OpWrite, addr, []uint16{value}, FaultNone)
		return err
	}

	// 设备模型可在掉电等状态下主动拒绝写入。
	if g, ok := b.model.(WriteGate); ok && !g.AllowWrite(RegisterView{regs: b.regs}, addr, value, b.clk()) {
		b.record(start, OpWrite, addr, []uint16{value}, FaultReject)
		return ErrRejected
	}

	fault := FaultNone
	if r := b.matchFault(OpWrite, addr); r != nil {
		switch r.Fault {
		case FaultTimeout:
			b.record(start, OpWrite, addr, []uint16{value}, FaultTimeout)
			if err := sleepCtx(ctx, r.Delay); err != nil {
				return err
			}
			return ErrTimeout
		case FaultReject:
			// 拒绝：内容不得落到寄存器，且不触发设备模型。
			b.record(start, OpWrite, addr, []uint16{value}, FaultReject)
			return ErrRejected
		case FaultCorrupt:
			// 篡改：线上到达设备的值与主机发出的不一致，
			// 设备按篡改值更新，主机随后的回读必然失败。
			value = ^value
			fault = FaultCorrupt
		}
	}

	b.regs[addr] = value
	if b.model != nil {
		b.model.OnWrite(RegisterView{regs: b.regs}, addr, value, b.clk())
	}
	b.record(start, OpWrite, addr, []uint16{value}, fault)
	return nil
}

func (b *MemBus) ReadBurst(ctx context.Context, addr uint8, n int) ([]uint16, error) {
	start := b.clk()
	b.mu.Lock()
	defer b.mu.Unlock()

	if n <= 0 {
		return nil, errors.New("bus: burst length must be positive")
	}

	if err := sleepCtx(ctx, b.latency); err != nil {
		b.record(start, OpBurst, addr, nil, FaultNone)
		return nil, err
	}

	v := RegisterView{regs: b.regs}
	if b.model != nil {
		b.model.OnBurst(v, addr, n, b.clk())
	}
	out := make([]uint16, n)
	for i := 0; i < n; i++ {
		out[i] = b.regs[addr+uint8(i)]
	}

	if r := b.matchFault(OpBurst, addr); r != nil {
		switch r.Fault {
		case FaultTimeout:
			b.record(start, OpBurst, addr, nil, FaultTimeout)
			if err := sleepCtx(ctx, r.Delay); err != nil {
				return nil, err
			}
			return nil, ErrTimeout
		case FaultReject:
			b.record(start, OpBurst, addr, nil, FaultReject)
			return nil, ErrRejected
		case FaultCorrupt:
			for i := range out {
				out[i] = ^out[i]
			}
			b.record(start, OpBurst, addr, out, FaultCorrupt)
			return out, nil
		}
	}

	b.record(start, OpBurst, addr, out, FaultNone)
	return out, nil
}

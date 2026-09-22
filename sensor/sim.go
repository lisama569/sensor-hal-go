package sensor

import (
	"sync"
	"time"

	"github.com/lisama569/sensor-hal-go/bus"
)

// SimDevice 是挂在 bus.MemBus 上的确定性加速度传感器模型：
//   - 按配置的 ODR 周期在状态寄存器被读取时推进转换，生成三轴数据；
//   - 掉电（Ctrl=0）时拒绝一切写入；
//   - 可通过 SimulatePowerLoss 模拟突发掉电：控制/配置寄存器复位，
//     配置签名清零（驱动恢复时必须据此重新初始化）；
//   - SetStatus / 可强制置位状态位，用于制造 OVERRUN / 需重校准等质量标记。
//
// 模型在总线锁内回调，不做任何阻塞操作。
type SimDevice struct {
	clk func() time.Time

	mu           sync.Mutex
	seq          uint16
	lastConv     time.Time
	lastConsumed bool
	forceStatus  uint16
	forceSeq     bool
}

// NewSimDevice 创建模拟设备。chipID/version 对应的寄存器由调用方在总线
// 初始寄存器表中给出。
func NewSimDevice(clk func() time.Time) *SimDevice {
	if clk == nil {
		clk = time.Now
	}
	return &SimDevice{clk: clk, lastConv: clk()}
}

// SimulatePowerLoss 模拟突发掉电：Ctrl/ODR/FSR 复位、配置签名清零、
// 数据与状态寄存器清零。必须通过总线 Direct 在总线锁内执行。
func (s *SimDevice) SimulatePowerLoss(b *bus.MemBus) {
	b.Direct(func(v bus.RegisterView) {
		v.Set(AddrCtrl, 0)
		v.Set(AddrODR, 0)
		v.Set(AddrFSR, 0)
		v.Set(AddrConfSig, 0)
		v.Set(AddrStatus, 0)
		v.Set(AddrDataX, 0)
		v.Set(AddrDataY, 0)
		v.Set(AddrDataZ, 0)
		s.mu.Lock()
		s.lastConsumed = true
		s.forceStatus = 0
		s.mu.Unlock()
	})
}

// SetStatus 强制设置状态寄存器附加位（OVERRUN / RCAL），下次生成数据时生效。
func (s *SimDevice) SetStatus(b *bus.MemBus, mask uint16) {
	s.mu.Lock()
	s.forceStatus |= mask
	s.mu.Unlock()
	b.Direct(func(v bus.RegisterView) {
		v.Set(AddrStatus, v.Get(AddrStatus)|mask)
	})
}

// AllowWrite 实现 bus.WriteGate：
//   - 向 Ctrl 写任何电源控制值（上电 / 挂起组合 / 掉电）始终放行，
//     否则主机永远无法上电（鸡生蛋问题）；
//   - 其他寄存器只在上电后接受写入；
//   - 挂起（SUSPEND）状态下的业务写由驱动在发事务前拦截。
func (s *SimDevice) AllowWrite(v bus.RegisterView, addr uint8, value uint16, now time.Time) bool {
	if addr == AddrCtrl {
		return true
	}
	return v.Get(AddrCtrl)&CtrlPowerOn != 0
}

func (s *SimDevice) OnWrite(v bus.RegisterView, addr uint8, value uint16, now time.Time) {
	switch addr {
	case AddrCtrl:
		// 进入 ACTIVE（仅上电、不挂起）时重置转换节拍。
		if value == CtrlPowerOn {
			s.mu.Lock()
			s.lastConv = now
			s.lastConsumed = true
			s.mu.Unlock()
			v.Set(AddrStatus, 0)
		}
	case AddrConfSig:
		if value == configSignature {
			s.mu.Lock()
			s.lastConv = now
			s.lastConsumed = true
			s.forceStatus = 0
			s.mu.Unlock()
			v.Set(AddrStatus, 0)
		}
	}
}

func (s *SimDevice) OnRead(v bus.RegisterView, addr uint8, now time.Time) {
	if addr != AddrStatus {
		return
	}
	// 掉电或挂起时不产生数据。
	ctrl := v.Get(AddrCtrl)
	if ctrl&CtrlPowerOn == 0 || ctrl&CtrlSuspend != 0 {
		v.Set(AddrStatus, 0)
		return
	}
	s.maybeConvert(v, now)
}

func (s *SimDevice) OnBurst(v bus.RegisterView, addr uint8, n int, now time.Time) {
	if addr == AddrDataX && n >= 3 {
		// 帧已取走：清除就绪与覆盖锁存，保留 RCAL 等强制位；
		// 节拍锚点重置为当前时刻，下一帧从现在起算一个 ODR 周期。
		v.Set(AddrStatus, v.Get(AddrStatus)&^uint16(StatusDRDY|StatusOVR))
		s.mu.Lock()
		s.lastConsumed = true
		s.lastConv = now
		s.mu.Unlock()
	}
}

func odrPeriod(code uint16) time.Duration {
	hz, ok := decODR(code)
	if !ok {
		return time.Second / 10
	}
	return time.Second / time.Duration(hz)
}

// maybeConvert 按 ODR 节拍推进转换。时间可能跳过多个周期（测试或调度延迟），
// 每跳过一个未取走的周期都产生一次 OVERRUN；状态寄存器中保留最新一帧。
// OVERRUN 位被锁存，直到数据被突发读取时清零。
func (s *SimDevice) maybeConvert(v bus.RegisterView, now time.Time) {
	s.mu.Lock()
	period := odrPeriod(v.Get(AddrODR))
	elapsed := now.Sub(s.lastConv)
	if elapsed < period {
		force := s.forceStatus
		s.mu.Unlock()
		v.Set(AddrStatus, v.Get(AddrStatus)|force)
		return
	}
	periods := int(elapsed / period)
	for i := 0; i < periods; i++ {
		if !s.lastConsumed {
			v.Set(AddrStatus, v.Get(AddrStatus)|StatusOVR)
		}
		s.seq++
		s.lastConsumed = false
	}
	seq := s.seq
	x, y, z := triad(seq)
	v.Set(AddrDataX, uint16(x))
	v.Set(AddrDataY, uint16(y))
	v.Set(AddrDataZ, uint16(z))
	s.lastConv = s.lastConv.Add(time.Duration(periods) * period)
	force := s.forceStatus
	s.mu.Unlock()

	st := v.Get(AddrStatus) | StatusDRDY | force
	v.Set(AddrStatus, st)
}

// triad 生成确定性的三轴数据（与 seq 一一对应，便于断言）。
func triad(seq uint16) (int16, int16, int16) {
	x := int16(seq)
	y := int16(seq * 2)
	z := int16(1000 - int16(seq)*3)
	return x, y, z
}

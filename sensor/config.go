package sensor

import "fmt"

// Config 是传感器的运行配置（采样率与量程）。
// 字段语义在所有兼容的寄存器映射 Minor 版本上保持不变：Minor 更新只允许
// 在寄存器映射尾部新增字段，不得改变本结构对应字段的含义。
type Config struct {
	ODR Hz // 输出数据速率
	FSR G  // 满量程
}

// Hz 输出数据速率，单位 Hz。
type Hz int

// G 满量程，单位 g。
type G int

// 设备支持的枚举值。
const (
	ODR10  Hz = 10
	ODR50  Hz = 50
	ODR100 Hz = 100
	ODR200 Hz = 200
	ODR400 Hz = 400
)

const (
	FSR2  G = 2
	FSR4  G = 4
	FSR8  G = 8
	FSR16 G = 16
)

var validODR = map[Hz]bool{ODR10: true, ODR50: true, ODR100: true, ODR200: true, ODR400: true}
var validFSR = map[G]bool{FSR2: true, FSR4: true, FSR8: true, FSR16: true}

// Validate 校验配置是否在设备允许范围内。非法配置绝不发起总线事务。
func (c Config) Validate() error {
	if !validODR[c.ODR] {
		return fail(KindBadConfig, "config", fmt.Sprintf("unsupported ODR %d Hz (allowed: 10/50/100/200/400)", c.ODR), 0)
	}
	if !validFSR[c.FSR] {
		return fail(KindBadConfig, "config", fmt.Sprintf("unsupported FSR +/-%dg (allowed: 2/4/8/16)", c.FSR), 0)
	}
	return nil
}

// encODR / encFSR 把枚举值编码为寄存器码值。
func encODR(h Hz) uint16 {
	switch h {
	case ODR10:
		return 0
	case ODR50:
		return 1
	case ODR100:
		return 2
	case ODR200:
		return 3
	case ODR400:
		return 4
	default:
		return 0xFFFF
	}
}

func decODR(code uint16) (Hz, bool) {
	switch code {
	case 0:
		return ODR10, true
	case 1:
		return ODR50, true
	case 2:
		return ODR100, true
	case 3:
		return ODR200, true
	case 4:
		return ODR400, true
	default:
		return 0, false
	}
}

func encFSR(g G) uint16 {
	switch g {
	case FSR2:
		return 0
	case FSR4:
		return 1
	case FSR8:
		return 2
	case FSR16:
		return 3
	default:
		return 0xFFFF
	}
}

func decFSR(code uint16) (G, bool) {
	switch code {
	case 0:
		return FSR2, true
	case 1:
		return FSR4, true
	case 2:
		return FSR8, true
	case 3:
		return FSR16, true
	default:
		return 0, false
	}
}

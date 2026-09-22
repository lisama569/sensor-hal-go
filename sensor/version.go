package sensor

import "fmt"

// 版本寄存器无法读取（未编程/保留值）时的原始值。
const RawVersionUnreadable uint16 = 0xFFFF

// RegVersion 是寄存器映射版本，编码为 (Major<<8 | Minor)。
//
// 兼容规则（确定性，详见 CheckCompatibility）：
//   - 芯片 ID 未知             => 拒绝，原因 UnknownChip
//   - 版本寄存器为 0xFFFF      => 拒绝，原因 Unreadable
//   - Major 不同（更新或更旧） => 拒绝，原因 MajorMismatch
//   - Major 相同、设备 Minor 更新 => 接受（Minor 更新只允许在寄存器映射
//     尾部新增字段，既有字段地址与语义不得改变）
//   - Major 相同、设备 Minor 相同 => 接受
//   - Major 相同、设备 Minor 更旧 => 拒绝，原因 TooOld（设备缺少驱动依赖字段）
type RegVersion struct {
	Major uint8
	Minor uint8
}

// DecodeVersion 从版本寄存器原始值解析版本。
func DecodeVersion(raw uint16) RegVersion {
	return RegVersion{Major: uint8(raw >> 8), Minor: uint8(raw)}
}

// EncodeVersion 把版本编码为寄存器原始值。
func EncodeVersion(v RegVersion) uint16 {
	return uint16(v.Major)<<8 | uint16(v.Minor)
}

func (v RegVersion) String() string {
	return fmt.Sprintf("%d.%d", v.Major, v.Minor)
}

// CompatReason 描述兼容判定结论的细分原因（拒绝原因彼此可区分）。
type CompatReason int

const (
	CompatOK            CompatReason = iota
	CompatUnknownChip                // 芯片 ID 不在驱动已知列表
	CompatMajorMismatch              // Major 不一致（含更新的 Major）
	CompatTooOld                     // 同 Major 但设备 Minor 更旧
	CompatUnreadable                 // 版本寄存器无法读取/未编程
)

func (r CompatReason) String() string {
	switch r {
	case CompatUnknownChip:
		return "UNKNOWN_CHIP"
	case CompatMajorMismatch:
		return "MAJOR_MISMATCH"
	case CompatTooOld:
		return "DEVICE_TOO_OLD"
	case CompatUnreadable:
		return "VERSION_UNREADABLE"
	default:
		return "OK"
	}
}

// Compatibility 是版本兼容判定结果。
type Compatibility struct {
	OK      bool
	Reason  CompatReason
	Device  RegVersion
	Driver  RegVersion
	ChipID  uint16
	Message string
}

// CheckCompatibility 依据固定规则判定设备版本是否被驱动接受。
// 判定只取决于入参，不依赖任何全局状态，因此结果确定且可复现。
func CheckCompatibility(chipID uint16, rawVersion uint16, driver RegVersion, knownChipIDs map[uint16]bool) Compatibility {
	c := Compatibility{Driver: driver, ChipID: chipID, Device: DecodeVersion(rawVersion)}

	if !knownChipIDs[chipID] {
		c.Reason = CompatUnknownChip
		c.Message = fmt.Sprintf("unknown chip id 0x%04X", chipID)
		return c
	}
	if rawVersion == RawVersionUnreadable {
		c.Reason = CompatUnreadable
		c.Message = "device register-map version register is unreadable (0xFFFF)"
		return c
	}

	switch {
	case c.Device.Major != driver.Major:
		// 更新的 Major 同样拒绝：Major 变化允许重新定义既有字段语义，
		// 本驱动无法保证语义不被静默改变。
		c.Reason = CompatMajorMismatch
		c.Message = fmt.Sprintf("register-map major mismatch: device=%s driver=%s", c.Device, driver)
		return c
	case c.Device.Minor < driver.Minor:
		c.Reason = CompatTooOld
		c.Message = fmt.Sprintf("device register-map too old: device=%s driver requires >= %s", c.Device, driver)
		return c
	default:
		c.OK = true
		c.Reason = CompatOK
		if c.Device.Minor > driver.Minor {
			c.Message = fmt.Sprintf("device minor %s is newer than driver %s; accepted (existing fields keep semantics)", c.Device, driver)
		} else {
			c.Message = fmt.Sprintf("exact register-map version match %s", driver)
		}
		return c
	}
}

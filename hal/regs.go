package hal

// Register addresses. The layout is fixed for every register map version;
// newer versions may only append registers, never change the meaning of
// existing ones.
const (
	RegVersion   uint16 = 0x00 // RO: register map version reported by device
	RegDeviceID  uint16 = 0x01 // RO: device identification
	RegMagic     uint16 = 0x02 // RW: init marker, cleared on power loss
	RegCtrl      uint16 = 0x03 // RW: control (enable bit)
	RegRate      uint16 = 0x04 // RW: sample rate code
	RegGain      uint16 = 0x05 // RW: gain code
	RegDataHi    uint16 = 0x06 // RO: sample value high byte
	RegDataLo    uint16 = 0x07 // RO: sample value low byte
	RegStatus    uint16 = 0x08 // RO: status flags
	RegLowPower  uint16 = 0x09 // RW: low power mode (register map v2+)
	regSpaceSize        = 0x10
)

// magicInit is written to RegMagic by Init. A missing magic after resume
// proves the device lost its configuration (power loss) and must be
// re-initialized instead of reusing stale state.
const magicInit uint8 = 0xA5

// ctrlEnable is the bit in RegCtrl that starts conversions.
const ctrlEnable uint8 = 0x01

// RegMapVersion is the register map revision reported in RegVersion.
type RegMapVersion uint8

const (
	RegMapV1 RegMapVersion = 1 // base layout
	RegMapV2 RegMapVersion = 2 // adds RegLowPower; v1 fields unchanged
)

// maxSupportedVersion is the newest register map this driver understands.
const maxSupportedVersion = RegMapV2

// CompatDecision is the deterministic outcome of a compatibility check.
type CompatDecision int

const (
	// CompatAccepted means the device register map is fully supported.
	CompatAccepted CompatDecision = iota
	// CompatRejectedUnknown means the device reported version 0 or a
	// value outside the defined version space.
	CompatRejectedUnknown
	// CompatRejectedTooNew means the device is newer than this driver;
	// rejected deterministically rather than risking silent semantic
	// drift of existing fields.
	CompatRejectedTooNew
)

func (d CompatDecision) String() string {
	switch d {
	case CompatAccepted:
		return "accepted"
	case CompatRejectedUnknown:
		return "rejected-unknown-version"
	case CompatRejectedTooNew:
		return "rejected-too-new"
	default:
		return "invalid"
	}
}

// CompatResult carries the decision plus a distinguishable reason.
type CompatResult struct {
	Version  RegMapVersion
	Decision CompatDecision
	Reason   string
}

// CheckCompatibility applies the deterministic version policy:
//   - v1..maxSupportedVersion: accepted (newer known versions only append
//     registers, so existing config field semantics never change);
//   - 0 or undefined: rejected as unknown;
//   - > maxSupportedVersion: rejected as too new.
func CheckCompatibility(v RegMapVersion) CompatResult {
	switch {
	case v >= RegMapV1 && v <= maxSupportedVersion:
		return CompatResult{
			Version:  v,
			Decision: CompatAccepted,
			Reason:   "register map version supported",
		}
	case v > maxSupportedVersion:
		return CompatResult{
			Version:  v,
			Decision: CompatRejectedTooNew,
			Reason:   "device register map is newer than this driver supports",
		}
	default:
		return CompatResult{
			Version:  v,
			Decision: CompatRejectedUnknown,
			Reason:   "device reported an undefined register map version",
		}
	}
}

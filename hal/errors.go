// Package hal implements a sensor hardware abstraction layer backed by an
// in-memory simulated register bus. No real hardware or external services
// are required.
package hal

import "fmt"

// ErrorKind classifies every failure that can cross the HAL boundary so
// callers can distinguish causes programmatically.
type ErrorKind int

const (
	// KindTimeout indicates the bus did not answer within the deadline.
	KindTimeout ErrorKind = iota + 1
	// KindNack indicates the device actively refused the access.
	KindNack
	// KindReadbackMismatch indicates a register read back a value
	// different from what was written.
	KindReadbackMismatch
	// KindInvalidConfig indicates the caller supplied an illegal
	// configuration before any bus traffic happened.
	KindInvalidConfig
	// KindIncompatibleVersion indicates the register map version of the
	// device is unknown or newer than this driver supports.
	KindIncompatibleVersion
	// KindSuspended indicates an operation was attempted while the device
	// is suspended; it fails fast instead of blocking or serving stale data.
	KindSuspended
	// KindNotInitialized indicates an operation requiring initialization
	// was requested before Init succeeded (or after power loss wiped it).
	KindNotInitialized
)

func (k ErrorKind) String() string {
	switch k {
	case KindTimeout:
		return "timeout"
	case KindNack:
		return "device-nack"
	case KindReadbackMismatch:
		return "readback-mismatch"
	case KindInvalidConfig:
		return "invalid-config"
	case KindIncompatibleVersion:
		return "incompatible-version"
	case KindSuspended:
		return "suspended"
	case KindNotInitialized:
		return "not-initialized"
	default:
		return "unknown"
	}
}

// Error is the single normalized error type exposed by the HAL. All bus
// level failures are classified and wrapped into *Error before crossing the
// public API.
type Error struct {
	Kind ErrorKind // normalized classification
	Op   string    // high level operation, e.g. "probe", "sample"
	Reg  uint16    // register involved, if any
	Msg  string    // human readable detail
	Err  error     // underlying cause, if any
}

func (e *Error) Error() string {
	s := fmt.Sprintf("hal: %s failed: %s", e.Op, e.Kind)
	if e.Msg != "" {
		s += ": " + e.Msg
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

func (e *Error) Unwrap() error { return e.Err }

// KindOf extracts the ErrorKind from an error returned by this package,
// returning 0 if err is nil or not a HAL error.
func KindOf(err error) ErrorKind {
	if e, ok := err.(*Error); ok {
		return e.Kind
	}
	return 0
}

func wrap(kind ErrorKind, op string, reg uint16, msg string, err error) *Error {
	return &Error{Kind: kind, Op: op, Reg: reg, Msg: msg, Err: err}
}

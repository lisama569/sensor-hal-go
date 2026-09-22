package hal

import (
	"sync"
	"time"
)

// PowerState is the runtime power state of the device.
type PowerState int

const (
	// PowerUninitialized: probed (or not) but Init has not completed.
	PowerUninitialized PowerState = iota
	// PowerActive: initialized and able to sample.
	PowerActive
	// PowerSuspended: low power; operations fail fast with KindSuspended.
	PowerSuspended
)

func (s PowerState) String() string {
	switch s {
	case PowerUninitialized:
		return "uninitialized"
	case PowerActive:
		return "active"
	case PowerSuspended:
		return "suspended"
	default:
		return "invalid"
	}
}

// Quality marks the trustworthiness of a sample.
type Quality int

const (
	// QualityGood: fresh data read after a verified configuration.
	QualityGood Quality = iota
	// QualityDegraded: data read after a retried bus timeout; value is
	// valid but the transfer was not clean on the first attempt.
	QualityDegraded
)

func (q Quality) String() string {
	switch q {
	case QualityGood:
		return "good"
	case QualityDegraded:
		return "degraded"
	default:
		return "invalid"
	}
}

// Sample is one sensor reading with explicit time and quality markers.
type Sample struct {
	Value     int16     // signed 16-bit sensor value
	Timestamp time.Time // wall clock when the read completed
	Quality   Quality   // trustworthiness marker
	Seq       uint64    // monotonically increasing sequence number
}

// Config is the device configuration. Field semantics are identical for
// every register map version; v2 only adds optional fields.
type Config struct {
	SampleRateCode uint8 // written to RegRate; 1..15
	GainCode       uint8 // written to RegGain; 0..7
	LowPower       bool  // v2+ only; ignored-with-error on v1 devices
}

// Validate checks the config without touching the bus.
func (c Config) Validate() error {
	if c.SampleRateCode < 1 || c.SampleRateCode > 15 {
		return wrap(KindInvalidConfig, "validate", RegRate,
			"sample rate code must be in 1..15", nil)
	}
	if c.GainCode > 7 {
		return wrap(KindInvalidConfig, "validate", RegGain,
			"gain code must be in 0..7", nil)
	}
	return nil
}

// transition records one power state migration for observability.
type transition struct {
	From, To PowerState
	At       time.Time
}

// maxTransitions bounds the transition log so long runs stay flat in memory.
const maxTransitions = 64

// Device is the sensor driver. All methods are safe for concurrent use.
type Device struct {
	bus Bus
	cfg Config

	mu      sync.Mutex // serializes state transitions and bus config
	state   PowerState
	version RegMapVersion
	seq     uint64
	trail   []transition // bounded power state migration log

	maxRetries int // read retries on timeout; writes are never retried
}

// NewDevice creates a driver bound to bus. cfg is validated lazily by Init.
func NewDevice(bus Bus, cfg Config) *Device {
	return &Device{bus: bus, cfg: cfg, maxRetries: 2}
}

// State returns the current power state.
func (d *Device) State() PowerState {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.state
}

// Version returns the negotiated register map version (0 before Probe).
func (d *Device) Version() RegMapVersion {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.version
}

// Transitions returns the recorded power state migrations, oldest first.
func (d *Device) Transitions() [][2]PowerState {
	d.mu.Lock()
	defer d.mu.Unlock()
	out := make([][2]PowerState, len(d.trail))
	for i, t := range d.trail {
		out[i] = [2]PowerState{t.From, t.To}
	}
	return out
}

// setState records a state migration. Caller must hold d.mu.
func (d *Device) setState(to PowerState) {
	if d.state == to {
		return
	}
	d.trail = append(d.trail, transition{From: d.state, To: to, At: time.Now()})
	if len(d.trail) > maxTransitions {
		copy(d.trail, d.trail[len(d.trail)-maxTransitions:])
		d.trail = d.trail[:maxTransitions]
	}
	d.state = to
}

// normalize maps raw bus errors onto public error kinds.
func normalize(op string, reg uint16, err error) *Error {
	switch err {
	case errBusTimeout:
		return wrap(KindTimeout, op, reg, "bus did not answer", err)
	case errBusNack:
		return wrap(KindNack, op, reg, "device refused the access", err)
	default:
		return wrap(KindNack, op, reg, "unexpected bus error", err)
	}
}

// readReg reads one register, retrying timeouts. Retried reports whether
// at least one retry happened (used to mark degraded sample quality).
// NACKs are never retried: a device refusal is definitive, and retrying
// must never mask a failure or multiply side effects.
func (d *Device) readReg(op string, addr uint16) (val uint8, retried bool, err error) {
	for attempt := 0; ; attempt++ {
		val, err = d.bus.ReadReg(addr)
		if err == nil {
			return val, attempt > 0, nil
		}
		if err != errBusTimeout || attempt >= d.maxRetries {
			return 0, attempt > 0, normalize(op, addr, err)
		}
	}
}

// writeRegChecked writes once and verifies by readback. Writes are never
// retried, so a failure can never cause a duplicate side effect.
func (d *Device) writeRegChecked(op string, addr uint16, val uint8) error {
	if err := d.bus.WriteReg(addr, val); err != nil {
		return normalize(op, addr, err)
	}
	got, _, err := d.readReg(op, addr)
	if err != nil {
		return err
	}
	if got != val {
		return wrap(KindReadbackMismatch, op, addr,
			"wrote value but read back a different one", nil)
	}
	return nil
}

// Probe reads the device ID and register map version and applies the
// compatibility policy.
func (d *Device) Probe() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, _, err := d.readReg("probe", RegDeviceID); err != nil {
		return err
	}
	vraw, _, err := d.readReg("probe", RegVersion)
	if err != nil {
		return err
	}
	res := CheckCompatibility(RegMapVersion(vraw))
	if res.Decision != CompatAccepted {
		return wrap(KindIncompatibleVersion, "probe", RegVersion,
			res.Decision.String()+": "+res.Reason, nil)
	}
	d.version = res.Version
	return nil
}

// Init validates the config and programs the device, verifying every
// write by readback.
func (d *Device) Init() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if err := d.cfg.Validate(); err != nil {
		return err
	}
	if d.version == 0 {
		return wrap(KindNotInitialized, "init", RegVersion,
			"probe must succeed before init", nil)
	}
	if d.cfg.LowPower && d.version < RegMapV2 {
		return wrap(KindInvalidConfig, "init", RegLowPower,
			"low power field requires register map v2", nil)
	}
	if err := d.programLocked("init"); err != nil {
		return err
	}
	d.setState(PowerActive)
	return nil
}

// programLocked writes the full configuration and enables conversions.
// Caller must hold d.mu.
func (d *Device) programLocked(op string) error {
	if err := d.writeRegChecked(op, RegRate, d.cfg.SampleRateCode); err != nil {
		return err
	}
	if err := d.writeRegChecked(op, RegGain, d.cfg.GainCode); err != nil {
		return err
	}
	if d.version >= RegMapV2 {
		var lp uint8
		if d.cfg.LowPower {
			lp = 1
		}
		if err := d.writeRegChecked(op, RegLowPower, lp); err != nil {
			return err
		}
	}
	if err := d.writeRegChecked(op, RegMagic, magicInit); err != nil {
		return err
	}
	return d.writeRegChecked(op, RegCtrl, ctrlEnable)
}

// Sample reads one sample. Fails fast with KindSuspended while suspended.
// The device mutex is held across both data register reads so concurrent
// samplers never observe a torn hi/lo pair; there is no busy waiting.
func (d *Device) Sample() (Sample, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch d.state {
	case PowerSuspended:
		return Sample{}, wrap(KindSuspended, "sample", RegDataHi,
			"device is suspended; resume before sampling", nil)
	case PowerUninitialized:
		return Sample{}, wrap(KindNotInitialized, "sample", RegDataHi,
			"device is not initialized", nil)
	}
	hi, retriedHi, err := d.readReg("sample", RegDataHi)
	if err != nil {
		return Sample{}, err
	}
	lo, retriedLo, err := d.readReg("sample", RegDataLo)
	if err != nil {
		return Sample{}, err
	}
	q := QualityGood
	if retriedHi || retriedLo {
		q = QualityDegraded
	}
	d.seq++
	return Sample{
		Value:     int16(uint16(hi)<<8 | uint16(lo)),
		Timestamp: time.Now(),
		Quality:   q,
		Seq:       d.seq,
	}, nil
}

// Suspend disables conversions and moves the device to PowerSuspended.
// Idempotent: suspending an already suspended device is a no-op.
func (d *Device) Suspend() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch d.state {
	case PowerUninitialized:
		return wrap(KindNotInitialized, "suspend", RegCtrl,
			"device is not initialized", nil)
	case PowerSuspended:
		return nil
	}
	if err := d.writeRegChecked("suspend", RegCtrl, 0); err != nil {
		return err
	}
	d.setState(PowerSuspended)
	return nil
}

// Resume wakes the device. If the init magic is gone (power loss) the
// device is re-initialized from scratch instead of reusing stale state.
// Idempotent: resuming an active device is a no-op.
func (d *Device) Resume() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	switch d.state {
	case PowerUninitialized:
		return wrap(KindNotInitialized, "resume", RegCtrl,
			"device is not initialized", nil)
	case PowerActive:
		return nil
	}
	magic, _, err := d.readReg("resume", RegMagic)
	if err != nil {
		return err
	}
	if magic != magicInit {
		// Power was lost while suspended: configuration registers are
		// gone, so reprogram everything rather than trusting old state.
		if err := d.programLocked("resume-reinit"); err != nil {
			return err
		}
	} else {
		// Config survived: only re-enable conversions.
		if err := d.writeRegChecked("resume", RegCtrl, ctrlEnable); err != nil {
			return err
		}
	}
	d.setState(PowerActive)
	return nil
}

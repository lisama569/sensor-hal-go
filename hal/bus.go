package hal

import (
	"errors"
	"sync"
)

// errBusTimeout and errBusNack are the raw bus-level failure signals. They
// are normalized into *Error before crossing the public API.
var (
	errBusTimeout = errors.New("bus timeout")
	errBusNack    = errors.New("bus nack")
)

// Bus is the minimal register bus the driver talks to.
type Bus interface {
	ReadReg(addr uint16) (uint8, error)
	WriteReg(addr uint16, val uint8) error
}

// Access records one bus read/write stimulus for observability in tests.
type Access struct {
	Read  bool   // true for read, false for write
	Addr  uint16 // register address
	Value uint8  // value read or written
	Err   error  // raw bus error, nil on success
}

// AccessLog is a bounded ring buffer of recent bus accesses. Bounded so
// long-running sampling keeps a stable memory footprint.
type AccessLog struct {
	mu    sync.Mutex
	buf   []Access
	next  int
	count int
}

// NewAccessLog creates a log holding at most capacity entries.
func NewAccessLog(capacity int) *AccessLog {
	if capacity <= 0 {
		capacity = 256
	}
	return &AccessLog{buf: make([]Access, capacity)}
}

// Record appends one access, evicting the oldest when full.
func (l *AccessLog) Record(a Access) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf[l.next] = a
	l.next = (l.next + 1) % len(l.buf)
	if l.count < len(l.buf) {
		l.count++
	}
}

// Entries returns the recorded accesses in chronological order.
func (l *AccessLog) Entries() []Access {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := make([]Access, 0, l.count)
	for i := 0; i < l.count; i++ {
		idx := (l.next - l.count + i + len(l.buf)) % len(l.buf)
		out = append(out, l.buf[idx])
	}
	return out
}

// Faults describes fault injection for the simulated bus. Each field maps
// to one distinguishable failure mode.
type Faults struct {
	TimeoutAddrs map[uint16]bool // reads/writes to these addrs time out
	NackAddrs    map[uint16]bool // accesses to these addrs are refused
	CorruptAddr  uint16          // reads of this addr return corrupted data
	CorruptMask  uint8           // XOR mask applied on CorruptAddr reads
	CorruptReads bool            // enable corruption
}

// SimBus is an in-memory register bus standing in for real hardware.
type SimBus struct {
	mu     sync.Mutex
	regs   []uint8
	log    *AccessLog
	faults Faults
}

// NewSimBus creates a simulated bus with size registers, all zero.
func NewSimBus(size int, log *AccessLog) *SimBus {
	if log == nil {
		log = NewAccessLog(256)
	}
	return &SimBus{regs: make([]uint8, size), log: log}
}

// Log returns the access log of this bus.
func (b *SimBus) Log() *AccessLog { return b.log }

// SetFaults replaces the fault injection configuration.
func (b *SimBus) SetFaults(f Faults) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.faults = f
}

// Poke writes a register without logging or faults; used by tests to
// prepare device state (e.g. the version register).
func (b *SimBus) Poke(addr uint16, val uint8) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.regs[addr] = val
}

// Clear wipes all registers, simulating power loss.
func (b *SimBus) Clear() {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.regs {
		b.regs[i] = 0
	}
}

// ReadReg reads one register, applying fault injection.
func (b *SimBus) ReadReg(addr uint16) (uint8, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.faults.TimeoutAddrs[addr] {
		b.log.Record(Access{Read: true, Addr: addr, Err: errBusTimeout})
		return 0, errBusTimeout
	}
	if b.faults.NackAddrs[addr] {
		b.log.Record(Access{Read: true, Addr: addr, Err: errBusNack})
		return 0, errBusNack
	}
	val := b.regs[addr]
	if b.faults.CorruptReads && addr == b.faults.CorruptAddr {
		val ^= b.faults.CorruptMask
	}
	b.log.Record(Access{Read: true, Addr: addr, Value: val})
	return val, nil
}

// WriteReg writes one register, applying fault injection.
func (b *SimBus) WriteReg(addr uint16, val uint8) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.faults.TimeoutAddrs[addr] {
		b.log.Record(Access{Addr: addr, Value: val, Err: errBusTimeout})
		return errBusTimeout
	}
	if b.faults.NackAddrs[addr] {
		b.log.Record(Access{Addr: addr, Value: val, Err: errBusNack})
		return errBusNack
	}
	b.regs[addr] = val
	b.log.Record(Access{Addr: addr, Value: val})
	return nil
}

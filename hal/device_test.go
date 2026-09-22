package hal

import (
	"strings"
	"sync"
	"testing"
)

// newReadyDevice returns a probed, initialized v2 device on a clean bus.
func newReadyDevice(t *testing.T) (*Device, *SimBus) {
	t.Helper()
	bus := NewSimBus(regSpaceSize, NewAccessLog(64))
	bus.Poke(RegDeviceID, 0x42)
	bus.Poke(RegVersion, uint8(RegMapV2))
	d := NewDevice(bus, Config{SampleRateCode: 4, GainCode: 2})
	if err := d.Probe(); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if err := d.Init(); err != nil {
		t.Fatalf("init: %v", err)
	}
	return d, bus
}

func dumpStimuli(t *testing.T, bus *SimBus) {
	t.Helper()
	for _, a := range bus.Log().Entries() {
		op := "W"
		if a.Read {
			op = "R"
		}
		if a.Err != nil {
			t.Logf("stimulus %s reg=0x%02x val=0x%02x err=%v", op, a.Addr, a.Value, a.Err)
		} else {
			t.Logf("stimulus %s reg=0x%02x val=0x%02x", op, a.Addr, a.Value)
		}
	}
}

func dumpTransitions(t *testing.T, d *Device) {
	t.Helper()
	for _, tr := range d.Transitions() {
		t.Logf("transition %s -> %s", tr[0], tr[1])
	}
}

func TestNormalPath(t *testing.T) {
	d, bus := newReadyDevice(t)
	bus.Poke(RegDataHi, 0x12)
	bus.Poke(RegDataLo, 0x34)

	s, err := d.Sample()
	if err != nil {
		t.Fatalf("sample: %v", err)
	}
	if s.Value != 0x1234 {
		t.Fatalf("value = %#x, want 0x1234", s.Value)
	}
	if s.Quality != QualityGood {
		t.Fatalf("quality = %s, want good", s.Quality)
	}
	if s.Timestamp.IsZero() {
		t.Fatal("timestamp must be set")
	}
	if s.Seq != 1 {
		t.Fatalf("seq = %d, want 1", s.Seq)
	}
	dumpStimuli(t, bus)
	dumpTransitions(t, d)
}

func TestTimeoutIsDistinguishedAndRetried(t *testing.T) {
	d, bus := newReadyDevice(t)
	bus.SetFaults(Faults{TimeoutAddrs: map[uint16]bool{RegDataHi: true}})

	_, err := d.Sample()
	if KindOf(err) != KindTimeout {
		t.Fatalf("kind = %v, want timeout (err=%v)", KindOf(err), err)
	}
	// maxRetries=2 => 1 initial + 2 retries = 3 read attempts on RegDataHi.
	attempts := 0
	for _, a := range bus.Log().Entries() {
		if a.Read && a.Addr == RegDataHi && a.Err != nil {
			attempts++
		}
	}
	if attempts != 3 {
		t.Fatalf("read attempts = %d, want 3", attempts)
	}
	dumpStimuli(t, bus)

	// Timeout must not be masked: clearing the fault recovers cleanly.
	bus.SetFaults(Faults{})
	if _, err := d.Sample(); err != nil {
		t.Fatalf("sample after recovery: %v", err)
	}
}

func TestNackIsDistinguishedAndNotRetried(t *testing.T) {
	d, bus := newReadyDevice(t)
	bus.SetFaults(Faults{NackAddrs: map[uint16]bool{RegDataLo: true}})

	_, err := d.Sample()
	if KindOf(err) != KindNack {
		t.Fatalf("kind = %v, want nack (err=%v)", KindOf(err), err)
	}
	attempts := 0
	for _, a := range bus.Log().Entries() {
		if a.Read && a.Addr == RegDataLo {
			attempts++
		}
	}
	if attempts != 1 {
		t.Fatalf("nack retried %d times, want exactly 1 attempt", attempts)
	}
	dumpStimuli(t, bus)
}

func TestReadbackMismatch(t *testing.T) {
	bus := NewSimBus(regSpaceSize, NewAccessLog(64))
	bus.Poke(RegDeviceID, 0x42)
	bus.Poke(RegVersion, uint8(RegMapV2))
	d := NewDevice(bus, Config{SampleRateCode: 4, GainCode: 2})
	if err := d.Probe(); err != nil {
		t.Fatalf("probe: %v", err)
	}
	// Corrupt reads of RegRate so the write verify fails.
	bus.SetFaults(Faults{CorruptReads: true, CorruptAddr: RegRate, CorruptMask: 0xFF})

	err := d.Init()
	if KindOf(err) != KindReadbackMismatch {
		t.Fatalf("kind = %v, want readback-mismatch (err=%v)", KindOf(err), err)
	}
	// The failed write must not be retried: exactly one write to RegRate.
	writes := 0
	for _, a := range bus.Log().Entries() {
		if !a.Read && a.Addr == RegRate {
			writes++
		}
	}
	if writes != 1 {
		t.Fatalf("writes to RegRate = %d, want 1 (no duplicate side effects)", writes)
	}
	dumpStimuli(t, bus)
}

func TestInvalidConfig(t *testing.T) {
	bus := NewSimBus(regSpaceSize, NewAccessLog(64))
	bus.Poke(RegVersion, uint8(RegMapV2))
	d := NewDevice(bus, Config{SampleRateCode: 0, GainCode: 2})
	if err := d.Probe(); err != nil {
		t.Fatalf("probe: %v", err)
	}
	if err := d.Init(); KindOf(err) != KindInvalidConfig {
		t.Fatalf("kind = %v, want invalid-config (err=%v)", KindOf(err), err)
	}
	// Config is rejected before any bus traffic: no writes at all.
	for _, a := range bus.Log().Entries() {
		if !a.Read {
			t.Fatalf("unexpected write stimulus: %+v", a)
		}
	}
	// LowPower on a v1 device is rejected, not silently reinterpreted.
	bus2 := NewSimBus(regSpaceSize, NewAccessLog(64))
	bus2.Poke(RegVersion, uint8(RegMapV1))
	d2 := NewDevice(bus2, Config{SampleRateCode: 4, GainCode: 2, LowPower: true})
	if err := d2.Probe(); err != nil {
		t.Fatalf("probe v1: %v", err)
	}
	if err := d2.Init(); KindOf(err) != KindInvalidConfig {
		t.Fatalf("v1 lowpower kind = %v, want invalid-config (err=%v)", KindOf(err), err)
	}
}

func TestVersionCompatibility(t *testing.T) {
	cases := []struct {
		version  uint8
		wantKind ErrorKind
		wantMsg  string // empty means accepted
	}{
		{1, 0, ""},
		{2, 0, ""},
		{0, KindIncompatibleVersion, "rejected-unknown-version"},
		{3, KindIncompatibleVersion, "rejected-too-new"},
		{255, KindIncompatibleVersion, "rejected-too-new"},
	}
	for _, tc := range cases {
		bus := NewSimBus(regSpaceSize, NewAccessLog(64))
		bus.Poke(RegDeviceID, 0x42)
		bus.Poke(RegVersion, tc.version)
		d := NewDevice(bus, Config{SampleRateCode: 4, GainCode: 2})
		err := d.Probe()
		if tc.wantMsg == "" {
			if err != nil {
				t.Fatalf("v%d: probe: %v", tc.version, err)
			}
			continue
		}
		if KindOf(err) != tc.wantKind {
			t.Fatalf("v%d: kind = %v, want %v", tc.version, KindOf(err), tc.wantKind)
		}
		if !strings.Contains(err.Error(), tc.wantMsg) {
			t.Fatalf("v%d: reason %q missing %q", tc.version, err.Error(), tc.wantMsg)
		}
		t.Logf("v%d rejected: %v", tc.version, err)
	}
}

func TestSuspendBlocksSamplingFast(t *testing.T) {
	d, bus := newReadyDevice(t)
	if err := d.Suspend(); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	if d.State() != PowerSuspended {
		t.Fatalf("state = %s, want suspended", d.State())
	}
	if _, err := d.Sample(); KindOf(err) != KindSuspended {
		t.Fatalf("kind = %v, want suspended (err=%v)", KindOf(err), err)
	}
	if err := d.Resume(); err != nil {
		t.Fatalf("resume: %v", err)
	}
	if d.State() != PowerActive {
		t.Fatalf("state = %s, want active", d.State())
	}
	if _, err := d.Sample(); err != nil {
		t.Fatalf("sample after resume: %v", err)
	}
	dumpStimuli(t, bus)
	dumpTransitions(t, d)
}

func TestPowerLossDuringSuspendForcesReinit(t *testing.T) {
	d, bus := newReadyDevice(t)
	if err := d.Suspend(); err != nil {
		t.Fatalf("suspend: %v", err)
	}
	bus.Clear() // simulate power loss: all registers wiped
	if err := d.Resume(); err != nil {
		t.Fatalf("resume after power loss: %v", err)
	}
	// Config must have been reprogrammed: magic and rate rewritten.
	var magicOK, rateOK bool
	for _, a := range bus.Log().Entries() {
		if !a.Read && a.Addr == RegMagic && a.Value == magicInit {
			magicOK = true
		}
		if !a.Read && a.Addr == RegRate && a.Value == 4 {
			rateOK = true
		}
	}
	if !magicOK || !rateOK {
		t.Fatalf("resume did not reinitialize (magic=%v rate=%v)", magicOK, rateOK)
	}
	if _, err := d.Sample(); err != nil {
		t.Fatalf("sample after reinit: %v", err)
	}
	dumpStimuli(t, bus)
	dumpTransitions(t, d)
}

func TestConcurrentSampling(t *testing.T) {
	d, bus := newReadyDevice(t)
	bus.Poke(RegDataHi, 0xAB)
	bus.Poke(RegDataLo, 0xCD)

	const workers = 8
	const perWorker = 200
	var wg sync.WaitGroup
	errs := make(chan error, workers*perWorker)
	for w := 0; w < workers; w++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				s, err := d.Sample()
				if err != nil {
					errs <- err
					return
				}
				if s.Value != -0x5433 { // 0xABCD as int16
					errs <- errTornRead(s.Value)
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatalf("concurrent sample: %v", err)
	}
	// Sequence numbers must be dense: no lost or duplicated samples.
	last, err := d.Sample()
	if err != nil {
		t.Fatalf("final sample: %v", err)
	}
	if want := uint64(workers*perWorker + 1); last.Seq != want {
		t.Fatalf("seq = %d, want %d", last.Seq, want)
	}
}

func TestLongRunResourceStability(t *testing.T) {
	d, bus := newReadyDevice(t)
	for i := 0; i < 10000; i++ {
		if _, err := d.Sample(); err != nil {
			t.Fatalf("sample %d: %v", i, err)
		}
	}
	for i := 0; i < 100; i++ {
		if err := d.Suspend(); err != nil {
			t.Fatalf("suspend %d: %v", i, err)
		}
		if err := d.Resume(); err != nil {
			t.Fatalf("resume %d: %v", i, err)
		}
	}
	if n := len(bus.Log().Entries()); n > 64 {
		t.Fatalf("access log grew to %d, capacity 64", n)
	}
	if n := len(d.Transitions()); n > maxTransitions {
		t.Fatalf("transition log grew to %d, max %d", n, maxTransitions)
	}
}

type tornReadError int16

func errTornRead(v int16) error { return tornReadError(v) }

func (e tornReadError) Error() string {
	return "torn read: inconsistent hi/lo pair"
}

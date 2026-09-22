package sensor

import "testing"

var known = map[uint16]bool{ChipIDA: true}

func TestCompatibilityMatrix(t *testing.T) {
	driver := RegVersion{Major: 2, Minor: 1}
	cases := []struct {
		name   string
		chip   uint16
		raw    uint16
		wantOK bool
		reason CompatReason
	}{
		{"exact match", ChipIDA, EncodeVersion(RegVersion{2, 1}), true, CompatOK},
		{"newer minor accepted", ChipIDA, EncodeVersion(RegVersion{2, 3}), true, CompatOK},
		{"older minor rejected", ChipIDA, EncodeVersion(RegVersion{2, 0}), false, CompatTooOld},
		{"newer major rejected", ChipIDA, EncodeVersion(RegVersion{3, 0}), false, CompatMajorMismatch},
		{"older major rejected", ChipIDA, EncodeVersion(RegVersion{1, 9}), false, CompatMajorMismatch},
		{"unknown chip", 0xDEAD, EncodeVersion(RegVersion{2, 1}), false, CompatUnknownChip},
		{"unreadable version", ChipIDA, 0xFFFF, false, CompatUnreadable},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := CheckCompatibility(tc.chip, tc.raw, driver, known)
			if c.OK != tc.wantOK || c.Reason != tc.reason {
				t.Fatalf("got ok=%v reason=%s msg=%q; want ok=%v reason=%s",
					c.OK, c.Reason, c.Message, tc.wantOK, tc.reason)
			}
			t.Logf("chip=0x%04X device=%s => ok=%v reason=%s %s",
				tc.chip, c.Device, c.OK, c.Reason, c.Message)
		})
	}
}

func TestVersionRoundTrip(t *testing.T) {
	v := RegVersion{Major: 2, Minor: 15}
	if got := DecodeVersion(EncodeVersion(v)); got != v {
		t.Fatalf("round trip got %s", got)
	}
}

func TestConfigValidate(t *testing.T) {
	if err := (Config{ODR: ODR100, FSR: FSR4}).Validate(); err != nil {
		t.Fatalf("valid config rejected: %v", err)
	}
	err := (Config{ODR: Hz(99), FSR: FSR4}).Validate()
	if err == nil {
		t.Fatal("bad ODR must fail")
	}
	if se, ok := AsError(err); !ok || se.Kind != KindBadConfig {
		t.Fatalf("want KindBadConfig, got %v", err)
	}
	err = (Config{ODR: ODR100, FSR: G(32)}).Validate()
	if se, ok := AsError(err); !ok || se.Kind != KindBadConfig {
		t.Fatalf("want KindBadConfig, got %v", err)
	}
}

func TestCodecRoundTrip(t *testing.T) {
	for _, h := range []Hz{ODR10, ODR50, ODR100, ODR200, ODR400} {
		got, ok := decODR(encODR(h))
		if !ok || got != h {
			t.Fatalf("ODR codec %d", h)
		}
	}
	for _, g := range []G{FSR2, FSR4, FSR8, FSR16} {
		got, ok := decFSR(encFSR(g))
		if !ok || got != g {
			t.Fatalf("FSR codec %d", g)
		}
	}
	if _, ok := decODR(0xFFFF); ok {
		t.Fatal("invalid ODR code must fail")
	}
}

package hal

import "testing"

func TestRingCapacityEvictsOldest(t *testing.T) {
	r := newRing[int](3)
	if got := r.len(); got != 0 {
		t.Fatalf("empty len = %d", got)
	}
	for i := 1; i <= 3; i++ {
		v := i
		seq := r.push(func(seq uint64) int { _ = seq; return v })
		if seq != uint64(i) {
			t.Fatalf("seq = %d, want %d", seq, i)
		}
	}
	got := r.snapshot(0)
	if len(got) != 3 || got[0] != 1 || got[1] != 2 || got[2] != 3 {
		t.Fatalf("snapshot before wrap = %v", got)
	}
	if r.dropped() != 0 {
		t.Fatalf("dropped = %d, want 0", r.dropped())
	}

	// 继续推入：容量恒定为 3，最老的被按时间顺序挤出。
	for i := 4; i <= 10; i++ {
		v := i
		r.push(func(seq uint64) int { _ = seq; return v })
	}
	if r.len() != 3 {
		t.Fatalf("len after overflow = %d, want 3", r.len())
	}
	got = r.snapshot(0)
	if len(got) != 3 || got[0] != 8 || got[1] != 9 || got[2] != 10 {
		t.Fatalf("snapshot after overflow = %v, want [8 9 10]", got)
	}
	if r.oldestSeq() != 8 || r.newestSeq() != 10 {
		t.Fatalf("window = [%d,%d], want [8,10]", r.oldestSeq(), r.newestSeq())
	}
	if r.dropped() != 7 {
		t.Fatalf("dropped = %d, want 7", r.dropped())
	}
}

func TestRingSnapshotMax(t *testing.T) {
	r := newRing[int](5)
	for i := 1; i <= 5; i++ {
		v := i
		r.push(func(seq uint64) int { _ = seq; return v })
	}
	if got := r.snapshot(3); len(got) != 3 || got[0] != 3 || got[2] != 5 {
		t.Fatalf("snapshot(3) = %v, want [3 4 5]", got)
	}
	if got := r.snapshot(0); len(got) != 5 {
		t.Fatalf("snapshot(0) = %v, want all 5", got)
	}
	// 挤掉之后再截取，逻辑顺序仍然正确（回绕路径）。
	for i := 6; i <= 9; i++ {
		v := i
		r.push(func(seq uint64) int { _ = seq; return v })
	}
	got := r.snapshot(3)
	if len(got) != 3 || got[0] != 7 || got[1] != 8 || got[2] != 9 {
		t.Fatalf("wrapped snapshot(3) = %v, want [7 8 9]", got)
	}
}

func TestRingSnapshotIsCopy(t *testing.T) {
	r := newRing[int](2)
	r.push(func(seq uint64) int { return 42 })
	got := r.snapshot(0)
	got[0] = 999
	again := r.snapshot(0)
	if again[0] != 42 {
		t.Fatalf("snapshot must be a copy, ring now holds %v", again)
	}
}

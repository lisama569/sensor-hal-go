package hal

// ring 是容量固定的环形缓冲：超过容量后按时间顺序把最老的条目挤出，
// 长时间运行下条目数（<=cap）与底层数组大小（==cap）保持平稳。
//
// 条目按推入顺序获得从 1 开始的累计序号（total）：即使最老条目已被挤出，
// 序号仍单调递增、绝不复用，调用方可据此判断自己错过的区间。
//
// ring 自身不加锁：由持有它的 Observer 统一在外层加锁。
type ring[T any] struct {
	items []T
	cap   int
	head  int    // 最老条目在 items 中的下标
	size  int    // 当前保留条目数（<=cap）
	total uint64 // 累计推入条数（含已被挤出的）
}

// newRing 创建容量为 capacity 的环形缓冲。capacity 必须为正。
func newRing[T any](capacity int) *ring[T] {
	return &ring[T]{
		items: make([]T, capacity),
		cap:   capacity,
	}
}

// push 追加一条；缓冲满时最老条目被覆盖挤出（FIFO）。
// make 以本次条目分配到的累计序号（从 1 开始、单调不复用）构造条目，
// 保证入环的条目自带正确序号。返回该累计序号。
func (r *ring[T]) push(make func(seq uint64) T) uint64 {
	r.total++
	v := make(r.total)
	if r.size < r.cap {
		// 尚未写满：按 [0,size) 顺序摆放，snapshot 无需回绕处理。
		r.items[r.size] = v
		r.size++
		return r.total
	}
	// 已写满：head 位置即最老条目，原地覆盖并前移窗口。
	r.items[r.head] = v
	r.head = (r.head + 1) % r.cap
	return r.total
}

// len 返回当前保留条目数（0..cap）。
func (r *ring[T]) len() int { return r.size }

// dropped 返回因容量上限被挤出的条目数。
func (r *ring[T]) dropped() uint64 {
	if r.total <= uint64(r.size) {
		return 0
	}
	return r.total - uint64(r.size)
}

// seqAt 返回逻辑位置 idx（0=最老）上条目的累计序号。
// 条目序号与其推入次序一致：写满回绕后第 idx 条的序号 = total-(size-idx)+1。
func (r *ring[T]) seqAt(idx int) uint64 {
	return r.total - uint64(r.size-1-idx)
}

// oldestSeq 返回最老保留条目的累计序号（空缓冲为 0）。
func (r *ring[T]) oldestSeq() uint64 {
	if r.size == 0 {
		return 0
	}
	return r.seqAt(0)
}

// newestSeq 返回最新条目的累计序号（空缓冲为 0）。
func (r *ring[T]) newestSeq() uint64 {
	if r.size == 0 {
		return 0
	}
	return r.total
}

// at 返回逻辑位置 idx（0=最老）上的条目拷贝。
func (r *ring[T]) at(idx int) T {
	return r.items[(r.head+idx)%r.cap]
}

// snapshot 返回从最老到最新的拷贝，至多 max 条（max<=0 表示全部保留条目）。
// 始终返回新分配的切片，调用方修改不影响缓冲。
func (r *ring[T]) snapshot(max int) []T {
	n := r.size
	if max > 0 && max < n {
		// 只取最新的 max 条：跳过前面 n-max 个最老条目。
		return r.window(n-max, n)
	}
	return r.window(0, n)
}

// window 返回逻辑位置 [lo,hi) 上条目的拷贝（从老到新）。
func (r *ring[T]) window(lo, hi int) []T {
	out := make([]T, 0, hi-lo)
	for i := lo; i < hi; i++ {
		out = append(out, r.at(i))
	}
	return out
}

// Package observe 在 HAL 之上提供面向长期连续运行的观测层：
// 容量有界、占用平稳的近期采样环形缓存与失败记录，采集与读取互不阻塞。
package observe

// ring 是容量固定的环形缓冲区：容量在构造时确定，写入超过容量后
// 按时间顺序挤掉最老的元素，整体内存占用恒定（不随运行时长增长）。
type ring[T any] struct {
	buf   []T
	head  int // 最老元素下标
	count int
}

// newRing 创建容量为 cap（必须 > 0）的环形缓冲区。
func newRing[T any](cap int) *ring[T] {
	if cap < 1 {
		cap = 1
	}
	return &ring[T]{buf: make([]T, cap)}
}

// add 写入一个元素，必要时挤掉最老元素。
func (r *ring[T]) add(v T) {
	if r.count < len(r.buf) {
		r.buf[(r.head+r.count)%len(r.buf)] = v
		r.count++
		return
	}
	// 已满：覆盖最老元素，头指针前移一格。
	r.buf[r.head] = v
	r.head = (r.head + 1) % len(r.buf)
}

// len 返回当前元素数量（<= 容量）。
func (r *ring[T]) len() int { return r.count }

// cap 返回容量。
func (r *ring[T]) cap() int { return len(r.buf) }

// at 返回按时间顺序第 i（0=最老）个元素。
func (r *ring[T]) at(i int) T {
	if i < 0 || i >= r.count {
		var zero T
		return zero
	}
	return r.buf[(r.head+i)%len(r.buf)]
}

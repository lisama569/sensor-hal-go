package observe

import (
	"sync"
	"time"

	"github.com/lisama569/sensor-hal-go/hal"
)

// Status 表示一次增量读取的结果类别，用于区分"没有任何数据"与
// "请求的数据已被挤掉"两种语义完全不同的空结果。
type Status int

const (
	// StatusOK 正常返回零或多帧（零帧表示游标之后暂无新数据）。
	StatusOK Status = iota
	// StatusEmpty 启动至今确实还没有采到任何采样。
	StatusEmpty
	// StatusEvicted 游标指向的数据已经被更新的数据挤出缓存，
	// 调用方应放弃旧游标，可用 Recent 取当前快照后重新接续。
	StatusEvicted
)

func (s Status) String() string {
	switch s {
	case StatusEmpty:
		return "EMPTY"
	case StatusEvicted:
		return "EVICTED"
	default:
		return "OK"
	}
}

// Frame 是带单调序号与入缓存时间的一帧完整采样。序号从 1 开始，
// 在单个 Observer 生命周期内严格递增，与底层总线/设备无关，
// 因此读取方拿到的每一帧都能按序号校验完整性与顺序。
type Frame struct {
	Seq    int64
	Stored time.Time
	Sample hal.Sample
}

// Batch 是一次近期采样读取的结果。Frames 按时间顺序（老→新）排列，
// 每个 Frame 都是独立的值拷贝，返回后采集继续覆盖环形槽位也不会影响它。
type Batch struct {
	Status Status
	Frames []Frame
	// OldestSeq / LatestSeq 为读取时刻缓存中最老/最新帧的序号；
	// 无数据时均为 0。NextCursor 是本批最后一帧之后的游标，
	// 传给下一次 Since 即可只取增量。
	OldestSeq  int64
	LatestSeq  int64
	NextCursor int64
}

// Stats 是观测层当前的容量与累计计数。累计值只增不减，
// 可用来判断"挤掉过多少"（SamplesTotal - SampleCount）。
type Stats struct {
	SampleCapacity int
	SampleCount    int
	SamplesTotal   int64
	FailureCap     int
	FailureCount   int
	FailuresTotal  int64
	LatestSeq      int64
}

// buffer 是采集与读取共享的有界缓存（近期采样 + 失败记录）。
//
// 并发约定：所有方法在同一把互斥锁下完成，且临界区内只做
// 定长切片的值拷贝（O(请求条数)，上限为容量），不做任何 IO、
// 不等待采集方；因此查询不会打断或卡住后台采集，反之亦然。
type buffer struct {
	mu      sync.Mutex
	samples *ring[Frame]
	fails   *ring[Failure]
	sTotal  int64 // 累计成功采样数（即最新帧序号）
	fTotal  int64 // 累计失败数（即最新失败记录序号）
	now     func() time.Time
}

func newBuffer(sampleCap, failureCap int, now func() time.Time) *buffer {
	if now == nil {
		now = time.Now
	}
	return &buffer{
		samples: newRing[Frame](sampleCap),
		fails:   newRing[Failure](failureCap),
		now:     now,
	}
}

// addSample 追加一帧；超过容量时最老帧被挤出，总占用不增长。
func (b *buffer) addSample(s hal.Sample) {
	b.mu.Lock()
	b.sTotal++
	b.samples.add(Frame{Seq: b.sTotal, Stored: b.now(), Sample: s})
	b.mu.Unlock()
}

// addFailure 追加一条失败记录；超过容量时最老记录被挤出。
func (b *buffer) addFailure(f Failure) {
	b.mu.Lock()
	b.fTotal++
	if f.T.IsZero() {
		f.T = b.now()
	}
	f.Seq = b.fTotal
	b.fails.add(f)
	b.mu.Unlock()
}

// boundsLocked 返回当前缓存中最老/最新帧的序号（调用方持锁）。
func (b *buffer) boundsLocked() (oldest, latest int64) {
	n := b.samples.len()
	if n == 0 {
		return 0, 0
	}
	return b.sTotal - int64(n) + 1, b.sTotal
}

// snapshotLocked 拷贝环形槽中 [start,end) 的帧（调用方持锁）。
func (b *buffer) snapshotLocked(start, end int) []Frame {
	if end <= start {
		return []Frame{}
	}
	out := make([]Frame, 0, end-start)
	for i := start; i < end; i++ {
		out = append(out, b.samples.at(i))
	}
	return out
}

// Recent 返回最近至多 n 条采样（时间顺序，老→新）。
// n<=0 或 n 超过当前条数时返回缓存内全部采样。
// 从无采样时 Status 为 StatusEmpty。
func (b *buffer) Recent(n int) Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := b.samples.len()
	oldest, latest := b.boundsLocked()
	if count == 0 {
		return Batch{Status: StatusEmpty, Frames: []Frame{}}
	}
	start := 0
	if n > 0 && n < count {
		start = count - n
	}
	frames := b.snapshotLocked(start, count)
	return Batch{
		Status:     StatusOK,
		Frames:     frames,
		OldestSeq:  oldest,
		LatestSeq:  latest,
		NextCursor: latest,
	}
}

// Since 返回序号严格大于 cursor、至多 max 条的增量采样。
// cursor 必须取自上一次 Batch 的 NextCursor（首次传 0）。
//
// 状态语义：
//   - 从无采样：StatusEmpty；
//   - cursor 指向的帧已被挤出（cursor>0 且 cursor<OldestSeq）：StatusEvicted；
//   - 其余：StatusOK，无新增帧时 Frames 为空切片而非 nil。
func (b *buffer) Since(cursor int64, max int) Batch {
	b.mu.Lock()
	defer b.mu.Unlock()
	count := b.samples.len()
	oldest, latest := b.boundsLocked()
	if count == 0 {
		return Batch{Status: StatusEmpty, Frames: []Frame{}}
	}
	if cursor > 0 && cursor < oldest {
		return Batch{
			Status:     StatusEvicted,
			Frames:     []Frame{},
			OldestSeq:  oldest,
			LatestSeq:  latest,
			NextCursor: latest,
		}
	}
	// 游标未来（>latest）时无新增，不移动游标。
	if cursor > latest {
		return Batch{
			Status:     StatusOK,
			Frames:     []Frame{},
			OldestSeq:  oldest,
			LatestSeq:  latest,
			NextCursor: latest,
		}
	}
	start := int(cursor - oldest + 1)
	if start < 0 {
		start = 0
	}
	end := count
	if max > 0 && max < end-start {
		end = start + max
	}
	frames := b.snapshotLocked(start, end)
	next := latest
	if len(frames) > 0 {
		next = frames[len(frames)-1].Seq
	} else if cursor >= oldest && cursor <= latest {
		next = cursor
	}
	return Batch{
		Status:     StatusOK,
		Frames:     frames,
		OldestSeq:  oldest,
		LatestSeq:  latest,
		NextCursor: next,
	}
}

// Failures 返回当前保留的失败记录快照（时间顺序，老→新）。
func (b *buffer) Failures() []Failure {
	b.mu.Lock()
	defer b.mu.Unlock()
	out := make([]Failure, 0, b.fails.len())
	for i := 0; i < b.fails.len(); i++ {
		out = append(out, b.fails.at(i))
	}
	return out
}

// Stats 返回容量、当前条数与累计计数的一致快照。
func (b *buffer) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	return Stats{
		SampleCapacity: b.samples.cap(),
		SampleCount:    b.samples.len(),
		SamplesTotal:   b.sTotal,
		FailureCap:     b.fails.cap(),
		FailureCount:   b.fails.len(),
		FailuresTotal:  b.fTotal,
		LatestSeq:      b.sTotal,
	}
}

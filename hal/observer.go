// Package hal：长期运行观测层。
//
// 本文件在既有 HAL 之上叠加一层面向边缘采集进程（连续运行数日以上）的观测
// 能力：后台持续采集，把成功采样保存在容量明确、可配置的最近采样环形缓冲中，
// 把失败事件保存在同样定容的失败记录环形缓冲中；查询操作以快照方式返回，
// 不阻塞、不打断在途采集，也不要求采集方等待。
package hal

import (
	"context"
	"errors"
	"sync"
	"time"
)

// 默认容量与重连等待：非法/零值配置在 NewObserver 时归一化到这些值。
const (
	defaultSampleCapacity  = 1024
	defaultFailureCapacity = 128
	defaultRestartWait     = 100 * time.Millisecond
)

// LoggedSample 是带单调序号的一帧采样。序号在 Observer 生命周期内严格递增、
// 绝不复用；即便旧帧已被挤出，后续帧的序号也不会回绕，调用方据此判断自己
// 要的批次是否已被更新数据挤掉。
type LoggedSample struct {
	Seq    uint64
	Sample Sample
}

// FailureRecord 是一条失败记录，与最近采样一起用于现场复盘。
type FailureRecord struct {
	Seq      uint64 // 失败记录自身的单调序号（与采样序号空间独立）
	T        time.Time
	Code     Code // 归一化错误码：同一根因在任何调度时机结论一致
	Op       string
	Attempts int
	Message  string
}

// QueryStatus 标识一次增量查询的结果性质。
type QueryStatus int

const (
	// QueryNoData 表示 Observer 自启动以来还没有任何采样。
	QueryNoData QueryStatus = iota
	// QueryMissed 表示 cursor 指向的部分（或全部）采样已被挤出缓冲；
	// 返回值是当前仍保留的最新内容，调用方应以最后一帧的序号重置游标。
	QueryMissed
	// QueryCurrent 表示 cursor 之后目前还没有新采样。
	QueryCurrent
	// QueryOK 表示正常返回 cursor 之后的新采样。
	QueryOK
)

func (s QueryStatus) String() string {
	switch s {
	case QueryMissed:
		return "MISSED"
	case QueryCurrent:
		return "CURRENT"
	case QueryOK:
		return "OK"
	default:
		return "NO_DATA"
	}
}

// SampleBatch 是 Since 的查询结果。
type SampleBatch struct {
	Status QueryStatus
	// Samples 按时间从老到新，每条都是完整一致的一帧快照。
	Samples []LoggedSample
	// OldestSeq / LatestSeq 是缓冲当前保留窗口的边界（空缓冲均为 0）。
	OldestSeq uint64
	LatestSeq uint64
}

// ObserverConfig 配置观测层。
type ObserverConfig struct {
	// SampleCapacity 是最近采样保留条数上限，必须 > 0；否则取默认值。
	SampleCapacity int
	// FailureCapacity 是失败记录保留条数上限，必须 > 0；否则取默认值。
	FailureCapacity int
	// RestartWait 是采集流遇到终态结束（挂起/掉电等）后重建流前的等待，
	// 防止底层快速连续失败时空转；<=0 取默认值。等待可被 Close 打断。
	RestartWait time.Duration
	// Clock 用于失败记录时间戳；nil 时用系统墙钟。
	Clock func() time.Time
}

// Observer 在一个 HAL 上持续采集并保留最近采样与失败记录。
//
// 并发模型（约定）：
//   - 只有 run 一个长期 goroutine 负责采集，它复用 HAL.Stream/HAL.Read 的
//     帧级串行化；流在终态错误后关闭，等待 RestartWait 再重建，不忙等、不空转；
//   - 采集拿到完整一帧后才短暂持锁入环，读总线/等待数据期间不持锁，因此
//     查询绝不会打断或卡住在途采集，采集也不会等待查询方；
//   - 全部查询方法返回新分配的快照副本，可与采集及彼此并发调用；
//   - Close 取消采集上下文，run goroutine 必然退出（Close 后 done 关闭）。
type Observer struct {
	h   *HAL
	cfg ObserverConfig
	now func() time.Time

	mu       sync.RWMutex
	samples  *ring[LoggedSample]
	failures *ring[FailureRecord]

	startOnce sync.Once
	closeOnce sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
	done      chan struct{}
}

// NewObserver 在给定 HAL 上创建观测器；非法/零值容量与等待被归一化为默认值。
func NewObserver(h *HAL, cfg ObserverConfig) *Observer {
	if cfg.SampleCapacity <= 0 {
		cfg.SampleCapacity = defaultSampleCapacity
	}
	if cfg.FailureCapacity <= 0 {
		cfg.FailureCapacity = defaultFailureCapacity
	}
	if cfg.RestartWait <= 0 {
		cfg.RestartWait = defaultRestartWait
	}
	now := cfg.Clock
	if now == nil {
		now = time.Now
	}
	return &Observer{
		h:        h,
		cfg:      cfg,
		now:      now,
		samples:  newRing[LoggedSample](cfg.SampleCapacity),
		failures: newRing[FailureRecord](cfg.FailureCapacity),
		done:     make(chan struct{}),
	}
}

// Start 启动后台采集 goroutine。重复调用不重复启动（以第一次的 ctx 为准）。
// 调用方取消 ctx 或调用 Close 后采集停止。
func (o *Observer) Start(ctx context.Context) {
	o.startOnce.Do(func() {
		if ctx == nil {
			ctx = context.Background()
		}
		o.ctx, o.cancel = context.WithCancel(ctx)
		go o.run()
	})
}

// Close 停止后台采集并等待采集 goroutine 退出；不触碰设备电源状态。
// 未 Start 时也安全；重复调用幂等。
func (o *Observer) Close() {
	o.closeOnce.Do(func() {
		if o.cancel != nil {
			o.cancel()
		}
	})
	if o.ctx != nil {
		<-o.done
	}
}

// run 是唯一的长期采集 goroutine。
func (o *Observer) run() {
	defer close(o.done)
	for {
		if o.ctx.Err() != nil {
			return
		}
		ch := o.h.Stream(o.ctx)
		for item := range ch {
			if item.Err != nil {
				// Stream 因自身 ctx 结束而退出（Close/启动 ctx 取消）：
				// 这是调用方主动结束采集，不作为设备失败记录。设备侧挂起/
				// 关电不会走到这里——它们的 ctx（o.ctx）仍然存活，错误为
				// SUSPENDED/POWERED_OFF，应落入失败环。
				if o.ctx.Err() != nil {
					return
				}
				o.recordFailure(item.Err)
				break
			}
			o.recordSample(item.Sample)
		}
		if o.ctx.Err() != nil {
			return
		}
		// 挂起/掉电等终态：等待后重建流，等待期间休眠，不忙等。
		t := time.NewTimer(o.cfg.RestartWait)
		select {
		case <-t.C:
		case <-o.ctx.Done():
			t.Stop()
			return
		}
	}
}

// recordSample 把一帧完整采样追加进定容环（仅短暂持锁）。
// 帧序号由 ring 按推入顺序统一分配，单调递增、绝不复用。
func (o *Observer) recordSample(s Sample) {
	o.mu.Lock()
	o.samples.push(func(seq uint64) LoggedSample {
		return LoggedSample{Seq: seq, Sample: s}
	})
	o.mu.Unlock()
}

// recordFailure 把一次采集失败归一化后追加进定容环。
// 错误码直接沿用 HAL 的归一化结论：同一设备/总线行为在任何调度时机
// 得到同一 Code；设备等待窗口超时（TIMEOUT）、设备挂起打断采集
// （SUSPENDED）与调用方主动结束（CANCELED）三者天然可分——挂起打断在途
// 采样时 HAL 已按电源侧取消原因校正为 SUSPENDED，不会因为 ctx 被取消而
// 误记成 CANCELED。
func (o *Observer) recordFailure(err error) {
	rec := FailureRecord{T: o.now(), Code: CodeInternal, Message: err.Error()}
	var de *DeviceError
	if errors.As(err, &de) {
		rec.Code = de.Code
		rec.Op = de.Op
		rec.Attempts = de.Attempts
		rec.Message = de.Message
	}
	o.mu.Lock()
	o.failures.push(func(seq uint64) FailureRecord {
		rec.Seq = seq
		return rec
	})
	o.mu.Unlock()
}

// Recent 返回最近至多 max 条采样快照（从老到新）；max<=0 表示当前保留的全部。
// 始终返回非 nil 的新切片；尚无采样时长度为 0。查询不阻塞、不打断采集。
func (o *Observer) Recent(max int) []LoggedSample {
	o.mu.RLock()
	out := o.samples.snapshot(max)
	o.mu.RUnlock()
	return out
}

// Since 返回严格晚于 cursor 的采样，四种 QueryStatus 语义见类型文档：
//   - 从未有过采样：QueryNoData；
//   - cursor 指向的帧已被挤出（cursor != 0 且早于窗口最老序号）：QueryMissed；
//   - cursor 已是最新：QueryCurrent；
//   - 其余：QueryOK，附 cursor 之后、当前仍保留的全部帧。
//
// cursor 传 0 表示从头取（Observer 尚未产生任何帧时仍为 QueryNoData）。
func (o *Observer) Since(cursor uint64) SampleBatch {
	o.mu.RLock()
	defer o.mu.RUnlock()

	r := o.samples
	b := SampleBatch{
		Samples:   []LoggedSample{},
		OldestSeq: r.oldestSeq(),
		LatestSeq: r.newestSeq(),
	}
	if r.len() == 0 {
		b.Status = QueryNoData
		return b
	}
	switch {
	case cursor == 0:
		b.Status = QueryOK
		b.Samples = r.window(0, r.len())
	case cursor < r.oldestSeq():
		// 游标之后的一部分起点帧已被更新数据挤掉。
		b.Status = QueryMissed
		b.Samples = r.window(0, r.len())
	case cursor >= r.newestSeq():
		b.Status = QueryCurrent
	default:
		b.Status = QueryOK
		// 第 idx 逻辑位的序号 = oldestSeq + idx；首条 seq>cursor 的
		// 逻辑下标 = cursor - oldestSeq + 1。
		lo := int(cursor-r.oldestSeq()) + 1
		b.Samples = r.window(lo, r.len())
	}
	return b
}

// RecentFailures 返回最近至多 max 条失败记录快照（从老到新）；max<=0 为全部。
// 始终返回非 nil 新切片。
func (o *Observer) RecentFailures(max int) []FailureRecord {
	o.mu.RLock()
	out := o.failures.snapshot(max)
	o.mu.RUnlock()
	return out
}

// Stats 是观测层当前容量占用信息。
type Stats struct {
	SampleCount     int
	SampleCapacity  int
	SampleDropped   uint64 // 因容量上限被挤出的采样条数
	FailureCount    int
	FailureCapacity int
	FailureDropped  uint64
	LatestSampleSeq uint64
}

// Stats 返回当前容量占用快照。长期连续运行下 Count 不超过容量、
// Dropped 单调增长但缓冲占用平稳，不随运行时长线性上涨。
func (o *Observer) Stats() Stats {
	o.mu.RLock()
	defer o.mu.RUnlock()
	return Stats{
		SampleCount:     o.samples.len(),
		SampleCapacity:  o.samples.cap,
		SampleDropped:   o.samples.dropped(),
		FailureCount:    o.failures.len(),
		FailureCapacity: o.failures.cap,
		FailureDropped:  o.failures.dropped(),
		LatestSampleSeq: o.samples.newestSeq(),
	}
}

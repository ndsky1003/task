package task

import (
	"time"
)

type Option struct {
	ConcurrenceNum        uint32
	NormalTaskHandleDelta time.Duration
	OrderTaskHandleDelta  []time.Duration
	OrderLockTTL          time.Duration // 有序任务跨实例锁的租约时长
	OrderLockRetryDelta   time.Duration // 有序任务锁获取失败后的退避时间
}

func Options() *Option {
	return &Option{
		ConcurrenceNum:        10,
		NormalTaskHandleDelta: 1 * time.Second,
		OrderTaskHandleDelta: []time.Duration{
			200 * time.Millisecond,
			1 * time.Second,
			3 * time.Second,
			7 * time.Second,
			13 * time.Second,
			21 * time.Second,
			37 * time.Second,
			69 * time.Second,
		},
		OrderLockTTL:        10 * time.Minute,
		OrderLockRetryDelta: 5 * time.Second,
	}
}

// 最大并发数
func (this *Option) SetConcurrenceNum(a uint32) {
	if a > 1000 {
		a = 1000
	}
	this.ConcurrenceNum = a
}

// 无顺序的任务，执行错误，等待多少再次执行
func (this *Option) SetNormalTaskHandleDelta(a time.Duration) {
	this.NormalTaskHandleDelta = a
}

// 具有顺序的任务，需要强制执行完每一个，当其中一个报错的时候，重复执行的间隔
func (this *Option) SetOrderTaskHandleDelta(a []time.Duration) {
	if len(a) == 0 {
		return
	}
	this.OrderTaskHandleDelta = a
}

// 有序任务跨实例锁的租约时长。需大于处理单个有序任务类型的最长耗时。
func (this *Option) SetOrderLockTTL(a time.Duration) {
	if a > 0 {
		this.OrderLockTTL = a
	}
}

// 有序任务锁获取失败后的退避时间。
func (this *Option) SetOrderLockRetryDelta(a time.Duration) {
	if a > 0 {
		this.OrderLockRetryDelta = a
	}
}

func (this *Option) Merge(deltas ...*Option) *Option {
	for _, v := range deltas {
		this.merge(v)
	}
	return this
}

func (this *Option) merge(delta *Option) *Option {
	if delta.ConcurrenceNum != 0 {
		this.ConcurrenceNum = delta.ConcurrenceNum
	}

	if delta.NormalTaskHandleDelta != 0 {
		this.NormalTaskHandleDelta = delta.NormalTaskHandleDelta
	}

	if len(delta.OrderTaskHandleDelta) != 0 {
		this.OrderTaskHandleDelta = delta.OrderTaskHandleDelta
	}

	if delta.OrderLockTTL != 0 {
		this.OrderLockTTL = delta.OrderLockTTL
	}

	if delta.OrderLockRetryDelta != 0 {
		this.OrderLockRetryDelta = delta.OrderLockRetryDelta
	}
	return this
}

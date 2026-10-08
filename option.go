package task

import (
	"time"
)

type Option struct {
	ConcurrenceNum        uint32
	NormalTaskHandleDelta time.Duration
	OrderTaskHandleDelta  []time.Duration
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
	return this
}

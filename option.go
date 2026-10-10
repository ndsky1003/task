package task

import (
	"context"
	"time"

	"github.com/ndsky1003/task/task"
)

type Option struct {
	ConcurrenceNum        uint32
	OrderConcurrenceNum   uint32 // 有序任务并发数（同时处理的有序任务类型数上限）
	NormalTaskHandleDelta time.Duration
	OrderTaskHandleDelta  []time.Duration
	OrderTaskMaxRetry     uint32 // 有序任务处理失败后的最大尝试次数（含首次），超出则标记死信

	Context     context.Context         // 可选，任务处理的 base context（用于取消）
	TaskTimeout time.Duration           // 可选，单任务处理超时（0 表示不超时）
	OnDead      func(*task.Task, error) // 可选，任务重试耗尽标记死信时的回调
}

func Options() *Option {
	return &Option{
		ConcurrenceNum:        10,
		OrderConcurrenceNum:   10,
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
		OrderTaskMaxRetry: 8,
	}
}

// 最大并发数
func (this *Option) SetConcurrenceNum(a uint32) {
	if a > 1000 {
		a = 1000
	}
	this.ConcurrenceNum = a
}

// 有序任务并发数：同时处理的有序任务类型数上限（与普通任务并发数独立）。
func (this *Option) SetOrderConcurrenceNum(a uint32) {
	if a > 1000 {
		a = 1000
	}
	if a > 0 {
		this.OrderConcurrenceNum = a
	}
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

// 有序任务处理失败后的最大尝试次数（含首次），超出则标记死信。
func (this *Option) SetOrderTaskMaxRetry(a uint32) {
	if a > 0 {
		this.OrderTaskMaxRetry = a
	}
}

// 设置任务处理的 base context（用于取消所有在途任务处理）。
func (this *Option) SetContext(ctx context.Context) {
	if ctx != nil {
		this.Context = ctx
	}
}

// 设置单任务处理超时（0 表示不超时）。
func (this *Option) SetTaskTimeout(d time.Duration) {
	if d > 0 {
		this.TaskTimeout = d
	}
}

// 设置死信回调：任务重试耗尽标记死信时触发。
func (this *Option) SetOnDead(fn func(*task.Task, error)) {
	if fn != nil {
		this.OnDead = fn
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

	if delta.OrderConcurrenceNum != 0 {
		this.OrderConcurrenceNum = delta.OrderConcurrenceNum
	}

	if delta.NormalTaskHandleDelta != 0 {
		this.NormalTaskHandleDelta = delta.NormalTaskHandleDelta
	}

	if len(delta.OrderTaskHandleDelta) != 0 {
		this.OrderTaskHandleDelta = delta.OrderTaskHandleDelta
	}

	if delta.OrderTaskMaxRetry != 0 {
		this.OrderTaskMaxRetry = delta.OrderTaskMaxRetry
	}

	if delta.Context != nil {
		this.Context = delta.Context
	}

	if delta.TaskTimeout != 0 {
		this.TaskTimeout = delta.TaskTimeout
	}

	if delta.OnDead != nil {
		this.OnDead = delta.OnDead
	}
	return this
}

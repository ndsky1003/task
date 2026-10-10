package task

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndsky1003/task/task"
)

// TestOrderDoesNotStarveNormal 验证普通任务不被有序任务饿死：
// 有序任务与普通任务使用独立并发控制，即使有序任务占满自己的并发槽，普通任务仍能立即处理。
func TestOrderDoesNotStarveNormal(t *testing.T) {
	f := &fakeSerialize{}
	var normalHandled atomic.Bool
	op := &fnOperator{fn: func(tk *task.Task) error {
		if tk.Order {
			time.Sleep(time.Second) // 有序任务长时间阻塞
			return nil
		}
		normalHandled.Store(true)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(1)      // 普通任务并发 1
	opt.SetOrderConcurrenceNum(1) // 有序任务并发 1
	mgr := NewTaskMgr(f, op, opt)

	// 3 个不同 type 的有序任务（串行共约 3 秒）+ 1 个普通任务
	for i := 1; i <= 3; i++ {
		if err := mgr.Add(mkPayload("o"), task.Meta{Type: uint32(i), Order: true}); err != nil {
			t.Fatalf("Add 有序: %v", err)
		}
	}
	if err := mgr.Add(mkPayload("n"), task.Meta{Type: 100}); err != nil {
		t.Fatalf("Add 普通: %v", err)
	}

	// 普通任务应在 1 秒内被处理（若共享并发槽，则要等有序任务 ~3 秒后才轮到）
	waitFor(t, time.Second, func() bool { return normalHandled.Load() })

	mgr.Shutdown()
}

// TestOrderConcurrenceLimit 验证有序任务并发数上限（OrderConcurrenceNum）生效。
func TestOrderConcurrenceLimit(t *testing.T) {
	f := &fakeSerialize{}
	var running, peak, done atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		c := running.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		running.Add(-1)
		done.Add(1)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	opt.SetOrderConcurrenceNum(2)
	mgr := NewTaskMgr(f, op, opt)

	// 5 个不同 type 的有序任务
	for i := 1; i <= 5; i++ {
		if err := mgr.Add(mkPayload("o"), task.Meta{Type: uint32(i), Order: true}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	waitFor(t, 5*time.Second, func() bool { return done.Load() == 5 })
	if p := peak.Load(); p > 2 {
		t.Fatalf("有序任务并发峰值 %d 超过 OrderConcurrenceNum=2", p)
	}
	mgr.Shutdown()
}

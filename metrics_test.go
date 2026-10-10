package task

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndsky1003/task/task"
)

// ctxOperator 同时实现 IOperator 与 IContextOperator，用于验证 context 分支。
type ctxOperator struct {
	fn func(ctx context.Context, tk *task.Task) error
}

func (o *ctxOperator) HandleTask(t *task.Task) error {
	return o.fn(context.Background(), t)
}

func (o *ctxOperator) HandleTaskCtx(ctx context.Context, t *task.Task) error {
	return o.fn(ctx, t)
}

// TestStats 验证成功处理场景下的统计计数。
func TestStats(t *testing.T) {
	f := &fakeSerialize{}
	op := &fnOperator{fn: func(tk *task.Task) error { return nil }}
	opt := Options()
	opt.SetConcurrenceNum(4)
	mgr := NewTaskMgr(f, op, opt)

	const n = 10
	for i := 0; i < n; i++ {
		if err := mgr.Add(mkPayload(fmt.Sprintf("s-%d", i)), task.Meta{Type: 1}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	waitFor(t, 5*time.Second, func() bool { return mgr.Stats().Handled == n })

	s := mgr.Stats()
	if s.Added != n {
		t.Fatalf("Added 期望 %d, got %d", n, s.Added)
	}
	if s.Handled != n {
		t.Fatalf("Handled 期望 %d, got %d", n, s.Handled)
	}
	if s.Handling != 0 {
		t.Fatalf("Handling 期望 0, got %d", s.Handling)
	}
	mgr.Shutdown()
}

// TestStatsFailedDead 验证失败与死信计数。
func TestStatsFailedDead(t *testing.T) {
	f := &fakeSerialize{}
	op := &fnOperator{fn: func(tk *task.Task) error { return fmt.Errorf("永远失败") }}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	opt.SetOrderTaskMaxRetry(3)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(mkPayload("dead"), task.Meta{Type: 1, Order: true}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool { return mgr.Stats().Dead == 1 })
	if s := mgr.Stats(); s.Failed < 3 {
		t.Fatalf("Failed 期望 >= 3, got %d", s.Failed)
	}
	mgr.Shutdown()
}

// TestOnDeadCallback 验证死信回调被触发。
func TestOnDeadCallback(t *testing.T) {
	f := &fakeSerialize{}
	op := &fnOperator{fn: func(tk *task.Task) error { return fmt.Errorf("永远失败") }}

	var called atomic.Bool
	deadErr := make(chan error, 1)
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	opt.SetOrderTaskMaxRetry(3)
	opt.SetOnDead(func(t *task.Task, err error) {
		called.Store(true)
		deadErr <- err
	})
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(mkPayload("dead"), task.Meta{Type: 1, Order: true}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool { return called.Load() })
	if err := <-deadErr; err == nil {
		t.Fatal("死信回调 err 应为非 nil")
	}
	mgr.Shutdown()
}

// TestContextOperator 验证实现 IContextOperator 时优先走 HandleTaskCtx 分支。
func TestContextOperator(t *testing.T) {
	f := &fakeSerialize{}
	ctxCalled := make(chan struct{}, 1)
	op := &ctxOperator{fn: func(ctx context.Context, tk *task.Task) error {
		select {
		case ctxCalled <- struct{}{}:
		default:
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(mkPayload("x"), task.Meta{Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	select {
	case <-ctxCalled:
	case <-time.After(5 * time.Second):
		t.Fatal("IContextOperator 的 HandleTaskCtx 未被调用")
	}
	mgr.Shutdown()
}

// TestTaskTimeout 验证 TaskTimeout 生效：context 会在超时后取消。
func TestTaskTimeout(t *testing.T) {
	f := &fakeSerialize{}
	done := make(chan error, 1)
	op := &ctxOperator{fn: func(ctx context.Context, tk *task.Task) error {
		<-ctx.Done() // 阻塞直到超时
		done <- ctx.Err()
		return ctx.Err()
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetTaskTimeout(100 * time.Millisecond)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(mkPayload("x"), task.Meta{Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	select {
	case err := <-done:
		if err != context.DeadlineExceeded {
			t.Fatalf("期望 DeadlineExceeded, got %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("context 未在超时后取消")
	}
	mgr.Shutdown()
}

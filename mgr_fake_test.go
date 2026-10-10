package task

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskmgrstatus"
)

// ---------- 内存版 ITaskSerialize，用于确定性测试 task_mgr 控制流 ----------

type fakeSerialize struct {
	mu        sync.Mutex
	initTasks []*task.Task // 待处理队列（FIFO）
	removed   []*task.Task
	restored  []*task.Task

	nextErr      error
	forceHasNext bool
	hasNextErr   error
	addErr       error
	removeErr    error
	updateErr    error
	recoverErr   error

	removeFailTimes int // Remove 前 N 次失败
	removeSuccess   int // Remove 成功次数
	updateFailTimes int // UpdateStatus2Init 前 N 次失败
}

func (f *fakeSerialize) Recover() error { return f.recoverErr }

func (f *fakeSerialize) Add(t *task.Task) error {
	if f.addErr != nil {
		return f.addErr
	}
	f.mu.Lock()
	f.initTasks = append(f.initTasks, t)
	f.mu.Unlock()
	return nil
}

func (f *fakeSerialize) Next(exclude ...uint32) (*task.Task, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, t := range f.initTasks {
		if containsType(exclude, t.Type) {
			continue
		}
		f.initTasks = append(f.initTasks[:i], f.initTasks[i+1:]...)
		return t, nil
	}
	return nil, task.ErrNoTask
}

func (f *fakeSerialize) NextByType(tp uint32) (*task.Task, error) {
	if f.nextErr != nil {
		return nil, f.nextErr
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for i, t := range f.initTasks {
		if t.Type == tp {
			f.initTasks = append(f.initTasks[:i], f.initTasks[i+1:]...)
			return t, nil
		}
	}
	return nil, task.ErrNoTask
}

func (f *fakeSerialize) Remove(t *task.Task) error {
	f.mu.Lock()
	f.removed = append(f.removed, t)
	fail := f.removeFailTimes > 0
	if fail {
		f.removeFailTimes--
	}
	f.mu.Unlock()
	if fail {
		return errors.New("remove fail")
	}
	f.mu.Lock()
	f.removeSuccess++
	f.mu.Unlock()
	return f.removeErr
}

func (f *fakeSerialize) UpdateStatus2Init(t *task.Task) error {
	f.mu.Lock()
	fail := f.updateFailTimes > 0
	if fail {
		f.updateFailTimes--
	}
	f.mu.Unlock()
	if fail {
		return errors.New("update fail")
	}
	if f.updateErr != nil {
		return f.updateErr
	}
	f.mu.Lock()
	f.restored = append(f.restored, t)
	f.initTasks = append(f.initTasks, t) // 模拟还原为 Init 并重新入队
	f.mu.Unlock()
	return nil
}

func (f *fakeSerialize) HasNext(exclude ...uint32) (bool, error) {
	if f.hasNextErr != nil {
		return false, f.hasNextErr
	}
	if f.forceHasNext {
		return true, nil
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, t := range f.initTasks {
		if !containsType(exclude, t.Type) {
			return true, nil
		}
	}
	return false, nil
}

func containsType(types []uint32, t uint32) bool {
	for _, v := range types {
		if v == t {
			return true
		}
	}
	return false
}

// ---------- 基于 fakeSerialize 的控制流测试 ----------

// TestRunLoopNextErrorContinues Next 返回非 task.ErrNoTask 错误时循环不应停止或 panic。
func TestRunLoopNextErrorContinues(t *testing.T) {
	f := &fakeSerialize{nextErr: errors.New("db down")}
	op := &fnOperator{}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(f, op, opt)

	time.Sleep(200 * time.Millisecond)
	if mgr.status.Load() != uint32(taskmgrstatus.Start) {
		t.Fatal("Next 出错时 run_loop 不应停止")
	}
	mgr.Shutdown()
}

// TestStopReturnsFalseWhenHasNext 有任务可处理时 Stop 应返回 false。
func TestStopReturnsFalseWhenHasNext(t *testing.T) {
	f := &fakeSerialize{forceHasNext: true}
	op := &fnOperator{}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(f, op, opt)

	if mgr.Stop() {
		t.Fatal("HasNext=true 时 Stop 应返回 false")
	}
	if mgr.status.Load() != uint32(taskmgrstatus.Start) {
		t.Fatal("有任务待处理时状态应保持 Start")
	}
	mgr.Shutdown()
}

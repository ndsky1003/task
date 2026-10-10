package task

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
)

// TestNoTaskLostUnderConcurrentAdd 验证 run_loop 空转即将 Stop 时并发 Add 不会丢任务。
func TestNoTaskLostUnderConcurrentAdd(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_no_loss", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var handled atomic.Int64
	op := &fnOperator{fn: func(tk *task.Task) error {
		handled.Add(1)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	const total = 300
	var wg sync.WaitGroup
	for i := 0; i < total; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			time.Sleep(time.Duration(n%7) * time.Millisecond)
			if err := mgr.Add(mkPayload(fmt.Sprintf("t-%d", n)), task.Meta{Type: uint32(n % 3)}); err != nil {
				t.Errorf("Add: %v", err)
			}
		}(i)
	}
	wg.Wait()

	waitFor(t, 20*time.Second, func() bool { return handled.Load() == total })
	if got := handled.Load(); got != total {
		t.Fatalf("任务丢失: 期望处理 %d 个，实际 %d 个", total, got)
	}
	mgr.Shutdown()
}

// TestNoTaskLostSequentialAddEmptyRunLoop 快速连续 Add，让 run_loop 反复经历「处理→空转→Stop→重启」。
func TestNoTaskLostSequentialAddEmptyRunLoop(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_no_loss_seq", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var handled atomic.Int64
	op := &fnOperator{fn: func(tk *task.Task) error {
		handled.Add(1)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(4)
	mgr := NewTaskMgr(ser, op, opt)

	const total = 100
	for i := 0; i < total; i++ {
		if err := mgr.Add(mkPayload(fmt.Sprintf("s-%d", i)), task.Meta{Type: uint32(i % 3)}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	waitFor(t, 15*time.Second, func() bool { return handled.Load() == total })
	if got := handled.Load(); got != total {
		t.Fatalf("任务丢失: 期望处理 %d 个，实际 %d 个", total, got)
	}
	mgr.Shutdown()
}

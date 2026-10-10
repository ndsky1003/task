package task

import (
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskmgrstatus"
)

// TestAddFillsTimeFields add 应为零值 UpdateTime/CreateTime 自动填充。
func TestAddFillsTimeFields(t *testing.T) {
	f := &fakeSerialize{}
	op := &fnOperator{}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(f, op, opt)

	task := &task.Task{ID: "id-1", Type: 4, Data: []byte(`{"v":"t"}`)}
	if err := mgr.add(task); err != nil {
		t.Fatalf("add: %v", err)
	}
	if is_zero_time(task.UpdateTime) {
		t.Fatal("add 后 UpdateTime 应为非零")
	}
	if is_zero_time(task.CreateTime) {
		t.Fatal("add 后 CreateTime 应为非零")
	}
	mgr.Shutdown()
}

// TestOrderTasksDifferentTypesParallel 不同类型的有序任务应并行执行。
func TestOrderTasksDifferentTypesParallel(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_parallel", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	start := make(chan uint32, 2)
	release := make(chan struct{})
	op := &fnOperator{fn: func(tk *task.Task) error {
		start <- tk.Type
		<-release
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	now := time.Now()
	if err := mgr.add(&task.Task{ID: "p1", Type: 11, Order: true, CreateTime: now, Data: []byte(`{"v":"p1"}`)}); err != nil {
		t.Fatalf("add: %v", err)
	}
	if err := mgr.add(&task.Task{ID: "p2", Type: 12, Order: true, CreateTime: now, Data: []byte(`{"v":"p2"}`)}); err != nil {
		t.Fatalf("add: %v", err)
	}

	select {
	case <-start:
	case <-time.After(3 * time.Second):
		close(release)
		t.Fatal("第一个有序任务未启动")
	}
	select {
	case <-start:
	case <-time.After(500 * time.Millisecond):
		close(release)
		t.Fatal("不同 type 的有序任务未并行执行")
	}
	close(release)
}

// TestOrderTaskDeadAfterMaxRetry 有序任务持续失败，达到最大重试次数后放弃且不删除任务。
func TestOrderTaskDeadAfterMaxRetry(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_dead", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		count.Add(1)
		return fmt.Errorf("永远失败")
	}}
	opt := Options()
	opt.SetConcurrenceNum(5)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	opt.SetOrderTaskMaxRetry(3)
	mgr := NewTaskMgr(ser, op, opt)

	id := "dead-1"
	if err := mgr.add(&task.Task{ID: id, Type: 13, Order: true, CreateTime: time.Now(), Data: []byte(`{"v":"dead"}`)}); err != nil {
		t.Fatalf("add: %v", err)
	}

	waitFor(t, 10*time.Second, func() bool { return count.Load() >= 3 })
	if !docExists(db, coll, id) {
		t.Fatal("重试耗尽后任务不应被删除")
	}
	time.Sleep(500 * time.Millisecond)
	c1 := count.Load()
	time.Sleep(500 * time.Millisecond)
	if count.Load() != c1 {
		t.Fatalf("重试耗尽后不应继续重试: %d -> %d", c1, count.Load())
	}
}

// TestAddAfterStopRestarts Stop 后 Add 应能重新启动处理循环。
func TestAddAfterStopRestarts(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_restart", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	handled := make(chan struct{}, 1)
	op := &fnOperator{fn: func(tk *task.Task) error {
		handled <- struct{}{}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	waitFor(t, 5*time.Second, func() bool {
		return mgr.status.Load() == uint32(taskmgrstatus.Stop)
	})
	if !mgr.Stop() {
		t.Fatal("空闲时 Stop 应返回 true")
	}

	if err := mgr.Add(mkPayload("after-stop"), task.Meta{Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("Stop 后 Add 未能重新启动处理")
	}
}

// TestOrderTasksSameTypeSerial 同一类型多个有序任务应严格串行（并发峰值恒为 1）。
func TestOrderTasksSameTypeSerial(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_same", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var running, peak, done atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		c := running.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(20 * time.Millisecond)
		running.Add(-1)
		done.Add(1)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	base := time.Now()
	for i := 0; i < 5; i++ {
		if err := mgr.add(&task.Task{
			ID:         fmt.Sprintf("s-%d", i),
			Type:       20,
			Order:      true,
			CreateTime: base.Add(time.Duration(i) * time.Second),
			Data:       []byte(`{"v":"x"}`),
		}); err != nil {
			t.Fatalf("add: %v", err)
		}
	}

	waitFor(t, 10*time.Second, func() bool { return done.Load() == 5 })
	if p := peak.Load(); p != 1 {
		t.Fatalf("同类型有序任务应严格串行, 并发峰值 %d, 期望 1", p)
	}
}

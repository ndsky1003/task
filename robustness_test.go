package task

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskstatus"
)

// ---------- 修复 1：用户处理函数 panic 不崩溃进程 ----------

// TestHandleTaskPanicRecovered 普通任务处理函数 panic 应被捕获并转为重试，而非崩溃。
func TestHandleTaskPanicRecovered(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_panic_normal", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		if count.Add(1) == 1 {
			panic("业务代码 panic")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetNormalTaskHandleDelta(50 * time.Millisecond)
	mgr := NewTaskMgr(ser, op, opt)

	id := addTask(t, mgr, 1, false, "panic")

	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
}

// TestHandleTaskPanicInOrderTask 有序任务处理函数 panic 应被捕获并重试。
func TestHandleTaskPanicInOrderTask(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_panic_order", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		if count.Add(1) == 1 {
			panic("业务代码 panic")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetOrderTaskHandleDelta([]time.Duration{10 * time.Millisecond})
	mgr := NewTaskMgr(ser, op, opt)

	id := addTask(t, mgr, 1, true, "panic-order")

	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
}

// ---------- 修复 2：Remove / UpdateStatus2Init 失败自动重试 ----------

// TestRemoveRetryEventuallyDeletes 删除失败后应自动重试直至成功。
func TestRemoveRetryEventuallyDeletes(t *testing.T) {
	f := &fakeSerialize{removeFailTimes: 2}
	op := &fnOperator{fn: func(tk *task.Task) error { return nil }}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetNormalTaskHandleDelta(20 * time.Millisecond)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(mkPayload("x"), task.Meta{ID: "id-1", Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 3*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.removeSuccess >= 1
	})
	f.mu.Lock()
	removedCalls := len(f.removed)
	f.mu.Unlock()
	if removedCalls < 3 {
		t.Fatalf("期望至少 3 次删除尝试（2 失败 + 1 成功），实际 %d", removedCalls)
	}
}

// TestUpdateStatus2InitRetrySucceeds 状态还原临时失败后应重试并最终重新处理任务。
func TestUpdateStatus2InitRetrySucceeds(t *testing.T) {
	f := &fakeSerialize{updateFailTimes: 1}
	var count atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		count.Add(1)
		if count.Load() == 1 {
			return errors.New("handle fail")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	opt.SetNormalTaskHandleDelta(20 * time.Millisecond)
	mgr := NewTaskMgr(f, op, opt)

	if err := mgr.Add(mkPayload("x"), task.Meta{ID: "id-1", Type: 1}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	waitFor(t, 5*time.Second, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.removeSuccess >= 1
	})
	if c := count.Load(); c < 2 {
		t.Fatalf("期望任务被重新处理（>=2 次），实际 %d", c)
	}
}

// ---------- 修复 4：有序任务重试耗尽标记死信，不阻塞同类型 ----------

// TestIsPanicMarksDead 有序任务重试耗尽后应标记为 Dead，且不阻塞同类型其他任务。
func TestIsPanicMarksDead(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_dead", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	var failCount atomic.Int32
	op := &fnOperator{fn: func(tk *task.Task) error {
		p := payloadOf(tk)
		if p == "fail" {
			failCount.Add(1)
			return fmt.Errorf("永远失败")
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(5)
	opt.SetOrderTaskHandleDelta([]time.Duration{time.Nanosecond})
	opt.SetOrderTaskMaxRetry(3)
	mgr := NewTaskMgr(ser, op, opt)

	failID := primitive.NewObjectID().Hex()
	okID := primitive.NewObjectID().Hex()
	base := time.Now()
	bfail := mkPayload("fail")
	if err := mgr.add(&task.Task{ID: failID, Type: 50, Order: true, CreateTime: base, Data: bfail}); err != nil {
		t.Fatalf("add fail: %v", err)
	}
	bok := mkPayload("ok")
	if err := mgr.add(&task.Task{ID: okID, Type: 50, Order: true, CreateTime: base.Add(time.Second), Data: bok}); err != nil {
		t.Fatalf("add ok: %v", err)
	}

	// fail 任务重试耗尽（3 次）
	waitFor(t, 10*time.Second, func() bool { return failCount.Load() >= 3 })
	// ok 任务应被正常处理删除，说明死信未阻塞同类型
	waitFor(t, 5*time.Second, func() bool { return !docExists(db, coll, okID) })

	var doc task.Task
	if err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": failID}).Decode(&doc); err != nil {
		t.Fatalf("查询 fail 任务: %v", err)
	}
	if doc.Status != taskstatus.Dead {
		t.Fatalf("重试耗尽后任务状态应为 Dead, got %v", doc.Status)
	}
}

// ---------- 修复 6：优雅关闭 ----------

// TestShutdownGraceful Shutdown 应等待正在处理的任务完成后返回。
func TestShutdownGraceful(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_shutdown", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	started := make(chan struct{}, 1)
	var handled atomic.Bool
	op := &fnOperator{fn: func(tk *task.Task) error {
		started <- struct{}{}
		time.Sleep(200 * time.Millisecond)
		handled.Store(true)
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	id := addTask(t, mgr, 1, false, "s")
	<-started

	mgr.Shutdown()

	if !handled.Load() {
		t.Fatal("Shutdown 应等待正在处理的任务完成")
	}
	if docExists(db, coll, id) {
		t.Fatal("Shutdown 后任务应已处理并删除")
	}
}

// TestAddAfterShutdownReturnsError Shutdown 后 Add 应返回错误。
func TestAddAfterShutdownReturnsError(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_shutdown_add", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)
	op := &fnOperator{}
	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	mgr.Shutdown()

	if err := mgr.Add(mkPayload("x"), task.Meta{Type: 1}); err == nil {
		t.Fatal("Shutdown 后 Add 应返回错误")
	}
}

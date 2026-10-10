package task

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskstatus"
)

// mkTask 构造测试任务。
func mkTask(id string, typ uint32, upd, create time.Time, order bool, v string) *task.Task {
	return &task.Task{
		ID:         id,
		Type:       typ,
		UpdateTime: upd,
		CreateTime: create,
		Order:      order,
		Data:       []byte(`{"v":"` + v + `"}`),
	}
}

// TestSerializeMongoNilClientPanic nil client 应 panic（无需连接）。
func TestSerializeMongoNilClientPanic(t *testing.T) {
	assertPanic(t, "client is nil", func() {
		serialize.Mongo(nil, "db", "c")
	})
}

// TestSerializeNextNoTask 空集合 Next 应返回 task.ErrNoTask。
func TestSerializeNextNoTask(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_no", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	if _, err := ser.Next(); !errors.Is(err, task.ErrNoTask) {
		t.Fatalf("期望 task.ErrNoTask, got %v", err)
	}
	if _, err := ser.NextByType(1); !errors.Is(err, task.ErrNoTask) {
		t.Fatalf("期望 task.ErrNoTask, got %v", err)
	}
}

// TestSerializeAddNextRemoveFlow Add -> Next -> Remove 全流程。
func TestSerializeAddNextRemoveFlow(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_flow", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	now := time.Now()
	if err := ser.Add(mkTask("flow-1", 1, now.Add(-time.Hour), now, false, "flow")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := ser.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.ID != "flow-1" {
		t.Fatalf("Next 返回错误任务: got %v", got.ID)
	}
	if err := ser.Remove(got); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if docExists(db, coll, "flow-1") {
		t.Fatal("Remove 后任务仍存在")
	}
}

// TestSerializeNextUpdatesStatusAndTime Next 后 Status 应为 Handling，UpdateTime 应被更新。
func TestSerializeNextUpdatesStatusAndTime(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_upd", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	oldTime := time.Now().Add(-time.Hour)
	if err := ser.Add(mkTask("upd-1", 5, oldTime, oldTime, false, "upd")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := ser.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	var doc task.Task
	if err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": "upd-1"}).Decode(&doc); err != nil {
		t.Fatalf("查询: %v", err)
	}
	if doc.Status != taskstatus.Handling {
		t.Fatalf("Next 后 Status 应为 Handling, got %v", doc.Status)
	}
	if !doc.UpdateTime.After(oldTime) {
		t.Fatalf("Next 后 UpdateTime 应更新")
	}
}

// TestSerializeNextExcludeTypes Next 应排除指定类型。
func TestSerializeNextExcludeTypes(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_excl", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	now := time.Now()
	if err := ser.Add(mkTask("t1", 1, now, now, false, "a")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := ser.Add(mkTask("t2", 2, now, now, false, "b")); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := ser.Next(1)
	if err != nil {
		t.Fatalf("Next(1): %v", err)
	}
	if got.ID != "t2" {
		t.Fatalf("Next(1) 应排除 type=1, got %v", got.ID)
	}
}

// TestSerializeNextByTypeOrderByCreateTime NextByType 应严格按 CreateTime 升序返回。
func TestSerializeNextByTypeOrderByCreateTime(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_bytype", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	base := time.Now()
	// 逆序插入，NextByType 按 CreateTime 升序返回
	ids := []string{"a", "b", "c"}
	for i := 0; i < 3; i++ {
		if err := ser.Add(mkTask(ids[i], 9, base, base.Add(time.Duration(2-i)*time.Second), false, ids[i])); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	want := []string{"c", "b", "a"} // CreateTime 升序
	for i := 0; i < 3; i++ {
		got, err := ser.NextByType(9)
		if err != nil {
			t.Fatalf("第 %d 次 NextByType: %v", i, err)
		}
		if got.ID != want[i] {
			t.Fatalf("第 %d 次 NextByType 顺序错误: got %v, want %v", i, got.ID, want[i])
		}
	}
}

// TestSerializeHasNext HasNext 应正确反映任务存在及类型排除。
func TestSerializeHasNext(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_has", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	if b, err := ser.HasNext(); err != nil || b {
		t.Fatalf("空集合 HasNext 应为 false, got %v err %v", b, err)
	}
	now := time.Now()
	_ = ser.Add(mkTask("h1", 1, now, now, false, "a"))
	_ = ser.Add(mkTask("h2", 2, now, now, false, "b"))
	if b, _ := ser.HasNext(); !b {
		t.Fatal("有任务 HasNext 应为 true")
	}
	if b, _ := ser.HasNext(1); !b {
		t.Fatal("排除 type=1 后仍有 type=2, HasNext(1) 应为 true")
	}
	if b, _ := ser.HasNext(1, 2); b {
		t.Fatal("排除 1,2 后 HasNext 应为 false")
	}
}

// TestSerializeConcurrentNextNoDuplicate 并发 Next 不应重复返回同一任务。
func TestSerializeConcurrentNextNoDuplicate(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_conc", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	const n = 50
	now := time.Now()
	for i := 0; i < n; i++ {
		if err := ser.Add(mkTask(string(rune('A'+i)), 10, now, now, false, "x")); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	var mu sync.Mutex
	got := map[string]bool{}
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tk, err := ser.Next(); err == nil {
				success.Add(1)
				mu.Lock()
				got[tk.ID] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if success.Load() == 0 {
		t.Fatal("并发 Next 未获取到任何任务")
	}
	if int(success.Load()) != len(got) {
		t.Fatalf("并发 Next 返回了重复任务: 成功 %d 次, 唯一 ID %d 个", success.Load(), len(got))
	}
}

// TestSerializeUpdateStatus2Init 应将 Handling 状态还原为 Init。
func TestSerializeUpdateStatus2Init(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_restore", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo(mongoClient, db, coll)

	now := time.Now()
	if err := ser.Add(mkTask("r1", 3, now, now, false, "x")); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := ser.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	if err := ser.UpdateStatus2Init(mkTask("r1", 3, now, now, false, "x")); err != nil {
		t.Fatalf("UpdateStatus2Init: %v", err)
	}
	var doc task.Task
	if err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": "r1"}).Decode(&doc); err != nil {
		t.Fatalf("查询: %v", err)
	}
	if doc.Status != taskstatus.Init {
		t.Fatalf("还原后 Status 应为 Init, got %v", doc.Status)
	}
}

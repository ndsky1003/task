package task

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/taskstatus"
)

// plainDoc 是刻意「不实现 itask.ITask」的结构体，用于验证运行时类型断言失败。
type plainDoc struct {
	ID     primitive.ObjectID `bson:"_id"`
	Status taskstatus.T       `bson:"Status"`
}

// TestSerializeMongoNilClientPanic nil client 应 panic（无需连接）。
func TestSerializeMongoNilClientPanic(t *testing.T) {
	assertPanic(t, "client is nil", func() {
		serialize.Mongo[testTask](nil, "db", "c")
	})
}

// TestSerializeNextNoTask 空集合 Next 应返回 ErrNoTask。
func TestSerializeNextNoTask(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_no", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	if _, err := ser.Next(); !errors.Is(err, itask.ErrNoTask) {
		t.Fatalf("期望 ErrNoTask, got %v", err)
	}
	if _, err := ser.NextByType(1); !errors.Is(err, itask.ErrNoTask) {
		t.Fatalf("期望 ErrNoTask, got %v", err)
	}
}

// TestSerializeAddNextRemoveFlow Add -> Next -> Remove 全流程。
func TestSerializeAddNextRemoveFlow(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_flow", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	id := primitive.NewObjectID()
	now := time.Now()
	tk := &testTask{ID: id, Type: 1, UpdateTime: now.Add(-time.Hour), CreateTime: now, Payload: "flow"}
	if err := ser.Add(tk); err != nil {
		t.Fatalf("Add: %v", err)
	}
	got, err := ser.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	if got.GetID() != id {
		t.Fatalf("Next 返回错误任务: got %v, want %v", got.GetID(), id)
	}
	if err := ser.Remove(got); err != nil {
		t.Fatalf("Remove: %v", err)
	}
	if docExists(db, coll, id) {
		t.Fatal("Remove 后任务仍存在")
	}
}

// TestSerializeNextUpdatesStatusAndTime Next 后 Status 应为 Handling，UpdateTime 应被更新。
func TestSerializeNextUpdatesStatusAndTime(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_upd", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	oldTime := time.Now().Add(-time.Hour)
	id := primitive.NewObjectID()
	if err := ser.Add(&testTask{ID: id, Type: 5, UpdateTime: oldTime, CreateTime: oldTime, Payload: "upd"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if _, err := ser.Next(); err != nil {
		t.Fatalf("Next: %v", err)
	}
	var doc testTask
	if err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": id}).Decode(&doc); err != nil {
		t.Fatalf("查询: %v", err)
	}
	if doc.Status != taskstatus.Handling {
		t.Fatalf("Next 后 Status 应为 Handling, got %v", doc.Status)
	}
	if !doc.UpdateTime.After(oldTime) {
		t.Fatalf("Next 后 UpdateTime 应更新, got %v (原 %v)", doc.UpdateTime, oldTime)
	}
}

// TestSerializeNextExcludeTypes Next 应排除指定类型。
func TestSerializeNextExcludeTypes(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_excl", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	now := time.Now()
	id1 := primitive.NewObjectID()
	id2 := primitive.NewObjectID()
	if err := ser.Add(&testTask{ID: id1, Type: 1, UpdateTime: now, CreateTime: now}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := ser.Add(&testTask{ID: id2, Type: 2, UpdateTime: now, CreateTime: now}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	got, err := ser.Next(1)
	if err != nil {
		t.Fatalf("Next(1): %v", err)
	}
	if got.GetID() != id2 {
		t.Fatalf("Next(1) 应排除 type=1, 返回 type=2 任务, got type=%d", got.GetType())
	}
}

// TestSerializeNextByTypeOrderByCreateTime NextByType 应严格按 CreateTime 升序返回。
func TestSerializeNextByTypeOrderByCreateTime(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_bytype", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	base := time.Now()
	ids := make([]primitive.ObjectID, 3)
	for i := 0; i < 3; i++ {
		ids[i] = primitive.NewObjectID()
		// 逆序插入：ids[0] 最新，ids[2] 最旧，按 CreateTime 应升序返回
		if err := ser.Add(&testTask{
			ID:         ids[i],
			Type:       9,
			CreateTime: base.Add(time.Duration(2-i) * time.Second),
			UpdateTime: base,
		}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		got, err := ser.NextByType(9)
		if err != nil {
			t.Fatalf("第 %d 次 NextByType: %v", i, err)
		}
		if got.GetID() != ids[2-i] {
			t.Fatalf("第 %d 次 NextByType 顺序错误: got %v, want %v", i, got.GetID(), ids[2-i])
		}
	}
}

// TestSerializeHasNext HasNext 应正确反映任务存在及类型排除。
func TestSerializeHasNext(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_has", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	if b, err := ser.HasNext(); err != nil || b {
		t.Fatalf("空集合 HasNext 应为 false, got %v err %v", b, err)
	}
	now := time.Now()
	if err := ser.Add(&testTask{ID: primitive.NewObjectID(), Type: 1, UpdateTime: now, CreateTime: now}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := ser.Add(&testTask{ID: primitive.NewObjectID(), Type: 2, UpdateTime: now, CreateTime: now}); err != nil {
		t.Fatalf("Add: %v", err)
	}
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

// TestSerializeConcurrentNextNoDuplicate 并发 Next 不应重复返回同一任务（验证条件更新的原子性）。
func TestSerializeConcurrentNextNoDuplicate(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_conc", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	const n = 50
	now := time.Now()
	for i := 0; i < n; i++ {
		if err := ser.Add(&testTask{ID: primitive.NewObjectID(), Type: 10, UpdateTime: now, CreateTime: now}); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	var mu sync.Mutex
	got := map[any]bool{}
	var success atomic.Int32
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if tk, err := ser.Next(); err == nil {
				success.Add(1)
				mu.Lock()
				got[tk.GetID()] = true
				mu.Unlock()
			}
		}()
	}
	wg.Wait()

	if success.Load() == 0 {
		t.Fatal("并发 Next 未获取到任何任务")
	}
	// 关键：成功返回次数 == 唯一 ID 数，即无重复
	if int(success.Load()) != len(got) {
		t.Fatalf("并发 Next 返回了重复任务: 成功 %d 次, 唯一 ID %d 个", success.Load(), len(got))
	}
	if len(got) > n {
		t.Fatalf("成功获取数 %d 超过任务数 %d", len(got), n)
	}
}

// TestSerializeTypeAssertFail 泛型 T 未实现 ITask 时 Next 应返回错误而非 panic。
func TestSerializeTypeAssertFail(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_assert", "tasks"
	dropCollection(t, db, coll)

	id := primitive.NewObjectID()
	if _, err := mongoClient.Database(db).Collection(coll).InsertOne(context.Background(), plainDoc{ID: id, Status: taskstatus.Init}); err != nil {
		t.Fatalf("插入: %v", err)
	}

	ser := serialize.Mongo[plainDoc](mongoClient, db, coll)
	_, err := ser.Next()
	if err == nil || !strings.Contains(err.Error(), "not implement ITask") {
		t.Fatalf("期望类型断言错误, got %v", err)
	}
}

// TestSerializeUpdateStatus2Init 应将 Handling 状态还原为 Init。
func TestSerializeUpdateStatus2Init(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_ser_restore", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	id := primitive.NewObjectID()
	now := time.Now()
	if err := ser.Add(&testTask{ID: id, Type: 3, UpdateTime: now, CreateTime: now}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	// 模拟进入处理中
	if _, err := mongoClient.Database(db).Collection(coll).UpdateOne(context.Background(),
		bson.M{"_id": id}, bson.M{"$set": bson.M{"Status": taskstatus.Handling}}); err != nil {
		t.Fatalf("模拟 Handling: %v", err)
	}

	if err := ser.UpdateStatus2Init(&testTask{ID: id}); err != nil {
		t.Fatalf("UpdateStatus2Init: %v", err)
	}
	var doc testTask
	if err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": id}).Decode(&doc); err != nil {
		t.Fatalf("查询: %v", err)
	}
	if doc.Status != taskstatus.Init {
		t.Fatalf("还原后 Status 应为 Init, got %v", doc.Status)
	}
}

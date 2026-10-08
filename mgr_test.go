package task

import (
	"context"
	"fmt"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/serialize"
	"github.com/ndsky1003/task/taskmgrstatus"
	"github.com/ndsky1003/task/taskstatus"
)

// ---------- 测试基础设施 ----------

var (
	mongoClient    *mongo.Client
	mongoAvailable bool
)

// mongoURI 支持通过环境变量覆盖，默认连本地。
func mongoURI() string {
	if u := os.Getenv("MONGO_URI"); u != "" {
		return u
	}
	return "mongodb://localhost:27017"
}

func TestMain(m *testing.M) {
	client, err := mongo.Connect(context.Background(), options.Client().ApplyURI(mongoURI()))
	if err == nil {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		err = client.Ping(ctx, nil)
		cancel()
		if err == nil {
			mongoClient = client
			mongoAvailable = true
		}
	}
	code := m.Run()
	if mongoClient != nil {
		_ = mongoClient.Disconnect(context.Background())
	}
	os.Exit(code)
}

func requireMongo(t *testing.T) {
	t.Helper()
	if !mongoAvailable {
		t.Skip("MongoDB 不可用，跳过集成测试")
	}
}

func dropCollection(t *testing.T, db, coll string) {
	t.Helper()
	requireMongo(t)
	_ = mongoClient.Database(db).Collection(coll).Drop(context.Background())
}

// waitFor 轮询等待条件满足，超时则失败。
func waitFor(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("条件在 %v 内未满足", timeout)
}

// docExists 判断指定 _id 的文档是否仍存在。查询出错时按不存在处理（交由测试超时兜底）。
func docExists(db, coll string, id primitive.ObjectID) bool {
	var d bson.M
	err := mongoClient.Database(db).Collection(coll).FindOne(context.Background(), bson.M{"_id": id}).Decode(&d)
	return err == nil
}

// assertPanic 断言 fn 会 panic 且信息包含 want。
func assertPanic(t *testing.T, want string, fn func()) {
	t.Helper()
	defer func() {
		r := recover()
		if r == nil {
			t.Fatalf("期望 panic(%q)，但没有 panic", want)
		}
		if !strings.Contains(fmt.Sprint(r), want) {
			t.Fatalf("panic 内容不匹配: got %v, want contains %q", r, want)
		}
	}()
	fn()
}

// ---------- 测试任务与操作器 ----------

// testTask 实现 itask.ITask，字段与 README 模板保持一致。
type testTask struct {
	ID         primitive.ObjectID `bson:"_id"`
	Type       uint32             `bson:"Type"`
	UpdateTime time.Time          `bson:"UpdateTime"`
	CreateTime time.Time          `bson:"CreateTime"`
	Status     taskstatus.T       `bson:"Status"`
	Order      bool               `bson:"Order"`
	Payload    string             `bson:"Payload"`
}

func (t *testTask) GetID() any                { return t.ID }
func (t *testTask) GetUpdateTime() time.Time  { return t.UpdateTime }
func (t *testTask) GetCreateTime() time.Time  { return t.CreateTime }
func (t *testTask) SetUpdateTime(a time.Time) { t.UpdateTime = a }
func (t *testTask) SetCreateTime(a time.Time) { t.CreateTime = a }
func (t *testTask) GetType() uint32           { return t.Type }
func (t *testTask) IsOrder() bool             { return t.Order }

// fnOperator 用闭包实现 operator.IOperator，便于按用例定制行为。
type fnOperator struct {
	fn func(itask.ITask) error
}

func (o *fnOperator) HandleTask(t itask.ITask) error {
	if o.fn == nil {
		return nil
	}
	return o.fn(t)
}

// ---------- 测试用例 ----------

// TestNewTaskMgrNilPanic 校验构造参数为 nil 时 panic。
func TestNewTaskMgrNilPanic(t *testing.T) {
	requireMongo(t)

	assertPanic(t, "task_serialize must not nil", func() {
		NewTaskMgr(nil, &fnOperator{})
	})

	db, coll := "task_test_nil", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)
	assertPanic(t, "op must not nil", func() {
		NewTaskMgr(ser, nil)
	})
}

// TestNormalTaskSuccessAndRemove 普通任务处理成功后应被删除。
func TestNormalTaskSuccessAndRemove(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_normal", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	handled := make(chan primitive.ObjectID, 16)
	op := &fnOperator{fn: func(tk itask.ITask) error {
		handled <- tk.GetID().(primitive.ObjectID)
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(4)
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{ID: id, Type: 1, Payload: "hello"}); err != nil {
		t.Fatalf("Add 失败: %v", err)
	}

	select {
	case got := <-handled:
		if got != id {
			t.Fatalf("处理的任务 ID 不符: got %v, want %v", got, id)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("任务未被处理")
	}

	// 处理成功后任务应被删除
	waitFor(t, 5*time.Second, func() bool { return !docExists(db, coll, id) })
}

// TestNormalTaskRetryThenSuccess 普通任务首次失败后应重试并最终成功删除。
func TestNormalTaskRetryThenSuccess(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_retry", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		if count.Add(1) == 1 {
			return fmt.Errorf("临时失败")
		}
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(1)
	opt.SetNormalTaskHandleDelta(50 * time.Millisecond)
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{ID: id, Type: 2, Payload: "retry"}); err != nil {
		t.Fatalf("Add 失败: %v", err)
	}

	// 至少重试 2 次且任务最终被删除
	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
	if n := count.Load(); n < 2 {
		t.Fatalf("期望至少处理 2 次，实际 %d", n)
	}
}

// TestOrderTaskSerialExecution 同一类型的有序任务应严格按 CreateTime 顺序串行执行。
func TestOrderTaskSerialExecution(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var mu sync.Mutex
	var order []string
	op := &fnOperator{fn: func(tk itask.ITask) error {
		tt := tk.(*testTask)
		mu.Lock()
		order = append(order, tt.Payload)
		mu.Unlock()
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	base := time.Now()
	for i, p := range []string{"a", "b", "c"} {
		if err := mgr.Add(&testTask{
			ID:         primitive.NewObjectID(),
			Type:       7,
			CreateTime: base.Add(time.Duration(i) * time.Second),
			Order:      true,
			Payload:    p,
		}); err != nil {
			t.Fatalf("Add 失败: %v", err)
		}
	}

	waitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(order) == 3
	})

	mu.Lock()
	defer mu.Unlock()
	want := []string{"a", "b", "c"}
	if len(order) != len(want) {
		t.Fatalf("处理数量不符: got %v, want %v", order, want)
	}
	for i := range want {
		if order[i] != want[i] {
			t.Fatalf("有序任务顺序错误: got %v, want %v", order, want)
		}
	}
}

// TestOrderTaskRetryThenSuccess 有序任务首次失败后应重试并最终成功。
func TestOrderTaskRetryThenSuccess(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_order_retry", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var count atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		if count.Add(1) == 1 {
			return fmt.Errorf("首次失败")
		}
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(5)
	opt.SetOrderTaskHandleDelta([]time.Duration{10 * time.Millisecond, 20 * time.Millisecond})
	mgr := NewTaskMgr(ser, op, opt)

	id := primitive.NewObjectID()
	if err := mgr.Add(&testTask{
		ID:         id,
		Type:       8,
		CreateTime: time.Now(),
		Order:      true,
		Payload:    "order-retry",
	}); err != nil {
		t.Fatalf("Add 失败: %v", err)
	}

	waitFor(t, 8*time.Second, func() bool {
		return count.Load() >= 2 && !docExists(db, coll, id)
	})
}

// TestConcurrentNormalTasks 多个普通任务应并发执行，且并发峰值不超过上限。
func TestConcurrentNormalTasks(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_concurrent", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	const maxWant = 5
	var cur, peak atomic.Int32
	var done atomic.Int32
	op := &fnOperator{fn: func(tk itask.ITask) error {
		c := cur.Add(1)
		for {
			p := peak.Load()
			if c <= p || peak.CompareAndSwap(p, c) {
				break
			}
		}
		time.Sleep(30 * time.Millisecond)
		cur.Add(-1)
		done.Add(1)
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(maxWant)
	mgr := NewTaskMgr(ser, op, opt)

	const n = 20
	for i := 0; i < n; i++ {
		if err := mgr.Add(&testTask{
			ID:      primitive.NewObjectID(),
			Type:    3,
			Payload: fmt.Sprintf("c-%d", i),
		}); err != nil {
			t.Fatalf("Add 失败: %v", err)
		}
	}

	waitFor(t, 10*time.Second, func() bool { return done.Load() == n })
	if p := peak.Load(); p > maxWant {
		t.Fatalf("并发峰值 %d 超过上限 %d", p, maxWant)
	}
	if p := peak.Load(); p < 2 {
		t.Fatalf("并发峰值 %d 过低，未体现并发能力", p)
	}
}

// TestMixedOrderAndNormal 有序任务与普通任务并存时都应被处理。
func TestMixedOrderAndNormal(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_mixed", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)

	var mu sync.Mutex
	handled := map[string]bool{}
	op := &fnOperator{fn: func(tk itask.ITask) error {
		tt := tk.(*testTask)
		mu.Lock()
		handled[tt.Payload] = true
		mu.Unlock()
		return nil
	}}

	opt := Options()
	opt.SetConcurrenceNum(10)
	mgr := NewTaskMgr(ser, op, opt)

	if err := mgr.Add(&testTask{
		ID:         primitive.NewObjectID(),
		Type:       1,
		Order:      true,
		CreateTime: time.Now().Add(-time.Second),
		Payload:    "ordered-1",
	}); err != nil {
		t.Fatalf("Add 失败: %v", err)
	}
	for i := 0; i < 5; i++ {
		if err := mgr.Add(&testTask{
			ID:      primitive.NewObjectID(),
			Type:    2,
			Payload: fmt.Sprintf("normal-%d", i),
		}); err != nil {
			t.Fatalf("Add 失败: %v", err)
		}
	}

	waitFor(t, 8*time.Second, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(handled) == 6
	})
}

// TestInitRestoreHandlingStatus 启动时应把异常退出遗留的「处理中」任务还原并继续处理。
func TestInitRestoreHandlingStatus(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_init", "tasks"
	dropCollection(t, db, coll)

	id := primitive.NewObjectID()
	_, err := mongoClient.Database(db).Collection(coll).InsertOne(context.Background(), testTask{
		ID:         id,
		Type:       9,
		UpdateTime: time.Now(),
		CreateTime: time.Now(),
		Status:     taskstatus.Handling,
		Payload:    "stale",
	})
	if err != nil {
		t.Fatalf("插入遗留任务失败: %v", err)
	}

	handled := make(chan struct{}, 1)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)
	op := &fnOperator{fn: func(tk itask.ITask) error {
		if tk.GetID() == id {
			handled <- struct{}{}
		}
		return nil
	}}
	opt := Options()
	opt.SetConcurrenceNum(2)
	_ = NewTaskMgr(ser, op, opt)

	select {
	case <-handled:
	case <-time.After(5 * time.Second):
		t.Fatal("遗留 Handling 任务未被还原并处理")
	}
}

// TestStopWhenIdle 无任务时空转的循环应自动停止，且再次 Stop 返回 true。
func TestStopWhenIdle(t *testing.T) {
	requireMongo(t)
	db, coll := "task_test_stop", "tasks"
	dropCollection(t, db, coll)
	ser := serialize.Mongo[testTask](mongoClient, db, coll)
	op := &fnOperator{fn: func(tk itask.ITask) error { return nil }}

	opt := Options()
	opt.SetConcurrenceNum(2)
	mgr := NewTaskMgr(ser, op, opt)

	waitFor(t, 5*time.Second, func() bool {
		return mgr.status.Load() == uint32(taskmgrstatus.Stop)
	})
	if !mgr.Stop() {
		t.Fatal("空闲状态 Stop() 应返回 true")
	}
}

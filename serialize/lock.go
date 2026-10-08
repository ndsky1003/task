package serialize

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/taskstatus"
)

// IOrderLocker 提供有序任务「类型级」的跨实例互斥锁。
// 实现此接口的序列化器可在多实例部署下保证同一类型的有序任务仅被一个实例串行处理。
// 未实现该接口时，task 库退化为「仅单实例内串行」。
type IOrderLocker interface {
	// TryLockOrderType 尝试获取类型 t 的处理锁。
	// owner 为实例唯一标识，ttl 为锁租约时长。
	// 返回 true 表示获取成功；false 表示锁被其他实例持有且未过期。
	TryLockOrderType(t uint32, owner string, ttl time.Duration) (bool, error)
	// UnlockOrderType 释放类型 t 的处理锁，仅 owner 匹配时生效。
	UnlockOrderType(t uint32, owner string) error
}

// IDeadLetter 提供死信标记能力：任务重试耗尽后标记为死信，等待人工处理。
type IDeadLetter interface {
	MarkDead(itask.ITask) error
}

// orderLockDoc 以 Type 作为 _id，天然唯一，保证同一类型同时只能持有一把锁。
type orderLockDoc struct {
	Type     uint32    `bson:"_id"`
	Owner    string    `bson:"Owner"`
	ExpireAt time.Time `bson:"ExpireAt"`
}

func (this *mongo_serialize[T]) lockCol() *mongo.Collection {
	return this.client.Database(this.database).Collection(this.coll + "_order_lock")
}

// TryLockOrderType 尝试获取类型 t 的跨实例锁。
// 利用 _id 唯一索引保证互斥；通过 ExpireAt 支持过期抢占，避免实例崩溃后锁永久残留。
func (this *mongo_serialize[T]) TryLockOrderType(t uint32, owner string, ttl time.Duration) (bool, error) {
	now := time.Now()
	doc := orderLockDoc{Type: t, Owner: owner, ExpireAt: now.Add(ttl)}

	_, err := this.lockCol().InsertOne(context.Background(), doc)
	if err == nil {
		return true, nil
	}
	if !mongo.IsDuplicateKeyError(err) {
		return false, err
	}

	// 锁已存在：先清理已过期的锁，再竞争插入（唯一索引保证仅一个实例成功）
	if _, err := this.lockCol().DeleteOne(context.Background(), bson.M{"_id": t, "ExpireAt": bson.M{"$lt": now}}); err != nil {
		return false, err
	}
	_, err = this.lockCol().InsertOne(context.Background(), doc)
	if err == nil {
		return true, nil
	}
	if mongo.IsDuplicateKeyError(err) {
		return false, nil
	}
	return false, err
}

// UnlockOrderType 释放类型 t 的锁，仅 owner 匹配时生效。
func (this *mongo_serialize[T]) UnlockOrderType(t uint32, owner string) error {
	_, err := this.lockCol().DeleteOne(context.Background(), bson.M{"_id": t, "Owner": owner})
	return err
}

// MarkDead 将任务标记为死信状态。
func (this *mongo_serialize[T]) MarkDead(t itask.ITask) error {
	_, err := this.col().UpdateByID(context.Background(), t.GetID(),
		bson.M{"$set": bson.M{"Status": taskstatus.Dead}})
	return err
}

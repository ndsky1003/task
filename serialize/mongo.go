package serialize

import (
	"context"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskstatus"
)

type mongo_serialize struct {
	client         *mongo.Client
	database, coll string
}

// Mongo 创建基于 MongoDB 的任务序列化器。构造函数会创建索引，失败即 panic。
func Mongo(client *mongo.Client, database, coll string) *mongo_serialize {
	if client == nil {
		panic("client is nil")
	}
	if _, err := client.
		Database(database).
		Collection(coll).
		Indexes().
		CreateMany(context.Background(), []mongo.IndexModel{
			{
				Keys:    bson.D{{Key: "Status", Value: 1}, {Key: "Type", Value: 1}, {Key: "UpdateTime", Value: 1}},
				Options: options.Index().SetUnique(false).SetSparse(false),
			},
			{
				Keys:    bson.D{{Key: "Status", Value: 1}, {Key: "Type", Value: 1}, {Key: "CreateTime", Value: 1}},
				Options: options.Index().SetUnique(false).SetSparse(false),
			},
		}); err != nil {
		panic(err)
	}

	return &mongo_serialize{
		client:   client,
		database: database,
		coll:     coll,
	}
}

func (this *mongo_serialize) col() *mongo.Collection {
	return this.client.Database(this.database).Collection(this.coll)
}

// Recover 恢复：将异常退出时遗留的“处理中”任务还原为初始状态，避免任务丢失
func (this *mongo_serialize) Recover() error {
	_, err := this.col().UpdateMany(context.Background(),
		bson.M{"Status": taskstatus.Handling},
		bson.M{"$set": bson.M{"Status": taskstatus.Init}})
	return err
}

func (this *mongo_serialize) Add(task *task.Task) error {
	_, err := this.col().InsertOne(context.Background(), task)
	return err
}

// initFilter 构造「待处理」任务的查询条件，可选排除某些类型。
func (this *mongo_serialize) initFilter(exclude ...uint32) bson.M {
	filter := bson.M{"Status": taskstatus.Init}
	if len(exclude) > 0 {
		arr := make([]uint32, len(exclude))
		copy(arr, exclude)
		filter["Type"] = bson.M{"$nin": arr}
	}
	return filter
}

// next 查询一个待处理任务并原子地将其置为「处理中」。
func (this *mongo_serialize) next(filter bson.M, sortKey string) (*task.Task, error) {
	var doc task.Task
	if err := this.col().FindOne(context.Background(), filter,
		options.FindOne().SetSort(bson.M{sortKey: 1})).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, task.ErrNoTask
		}
		return nil, err
	}

	res, err := this.col().UpdateOne(context.Background(),
		bson.M{"_id": doc.ID, "Status": taskstatus.Init},
		bson.M{"$set": bson.M{"UpdateTime": time.Now(), "Status": taskstatus.Handling}})
	if err != nil {
		return nil, err
	}
	if res.MatchedCount == 0 {
		return nil, task.ErrNoTask
	}
	return &doc, nil
}

func (this *mongo_serialize) Next(exclude_t ...uint32) (*task.Task, error) {
	return this.next(this.initFilter(exclude_t...), "UpdateTime")
}

func (this *mongo_serialize) NextByType(t uint32) (*task.Task, error) {
	return this.next(bson.M{"Status": taskstatus.Init, "Type": t}, "CreateTime")
}

func (this *mongo_serialize) HasNext(exclude_t ...uint32) (bool, error) {
	count, err := this.col().CountDocuments(
		context.Background(),
		this.initFilter(exclude_t...),
	)
	return count > 0, err
}

func (this *mongo_serialize) Remove(t *task.Task) error {
	_, err := this.col().DeleteOne(context.Background(), bson.M{"_id": t.ID})
	return err
}

// UpdateStatus2Init 任务状态还原，等待下次执行
func (this *mongo_serialize) UpdateStatus2Init(t *task.Task) error {
	_, err := this.col().UpdateByID(context.Background(),
		t.ID,
		bson.M{"$set": bson.M{"Status": taskstatus.Init}},
	)
	return err
}

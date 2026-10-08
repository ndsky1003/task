package serialize

import (
	"context"
	"errors"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ndsky1003/task/itask"
	"github.com/ndsky1003/task/taskstatus"
)

type mongo_serialize[T any] struct {
	client         *mongo.Client
	database, coll string
}

func Mongo[T any](client *mongo.Client, database, coll string) *mongo_serialize[T] {
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

	return &mongo_serialize[T]{
		client:   client,
		database: database,
		coll:     coll,
	}
}

func (this *mongo_serialize[T]) col() *mongo.Collection {
	return this.client.Database(this.database).Collection(this.coll)
}

// 初始化：将异常退出时遗留的“处理中”任务还原为初始状态，避免任务丢失
func (this *mongo_serialize[T]) Init() error {
	_, err := this.col().UpdateMany(context.Background(),
		bson.M{"Status": taskstatus.Handling},
		bson.M{"$set": bson.M{"Status": taskstatus.Init}})
	return err
}

func (this *mongo_serialize[T]) Add(task itask.ITask) error {
	_, err := this.col().InsertOne(context.Background(), task)
	return err
}

func (this *mongo_serialize[T]) Next(exclude_t ...uint32) (itask.ITask, error) {
	filter := bson.M{
		"Status": taskstatus.Init,
	}
	if len(exclude_t) > 0 {
		arr := make([]uint32, len(exclude_t))
		copy(arr, exclude_t)
		filter["Type"] = bson.M{"$nin": arr}
	}

	// 先查询并校验，成功后再更新状态，避免校验失败时任务卡在“处理中”
	var doc T
	if err := this.col().FindOne(context.Background(), filter,
		options.FindOne().SetSort(bson.M{"UpdateTime": 1})).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, itask.ErrNoTask
		}
		return nil, err
	}
	var d any = &doc
	task, ok := d.(itask.ITask)
	if !ok {
		return nil, errors.New("type doc is not implement ITask")
	}

	res, err := this.col().UpdateOne(context.Background(),
		bson.M{"_id": task.GetID(), "Status": taskstatus.Init},
		bson.M{"$set": bson.M{"UpdateTime": time.Now(), "Status": taskstatus.Handling}})
	if err != nil {
		return nil, err
	}
	if res.MatchedCount == 0 {
		return nil, itask.ErrNoTask
	}
	return task, nil
}

func (this *mongo_serialize[T]) NextByType(t uint32) (itask.ITask, error) {
	filter := bson.M{
		"Status": taskstatus.Init,
		"Type":   t,
	}

	// 先查询并校验，成功后再更新状态
	var doc T
	if err := this.col().FindOne(context.Background(), filter,
		options.FindOne().SetSort(bson.M{"CreateTime": 1})).Decode(&doc); err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, itask.ErrNoTask
		}
		return nil, err
	}
	var d any = &doc
	task, ok := d.(itask.ITask)
	if !ok {
		return nil, errors.New("type doc is not implement ITask")
	}

	res, err := this.col().UpdateOne(context.Background(),
		bson.M{"_id": task.GetID(), "Status": taskstatus.Init},
		bson.M{"$set": bson.M{"UpdateTime": time.Now(), "Status": taskstatus.Handling}})
	if err != nil {
		return nil, err
	}
	if res.MatchedCount == 0 {
		return nil, itask.ErrNoTask
	}
	return task, nil
}

func (this *mongo_serialize[T]) HasNext(exclude_t ...uint32) (bool, error) {
	filter := bson.M{
		"Status": taskstatus.Init,
	}
	if len(exclude_t) > 0 {
		arr := make([]uint32, len(exclude_t))
		copy(arr, exclude_t)
		filter["Type"] = bson.M{"$nin": arr}
	}
	count, err := this.col().CountDocuments(
		context.Background(),
		filter,
	)
	return count > 0, err
}

func (this *mongo_serialize[T]) Remove(t itask.ITask) error {
	_, err := this.col().DeleteOne(context.Background(), bson.M{"_id": t.GetID()})
	return err
}

// 任务状态还原。eg:如果任务出错，需要将状态还原，等待下次执行
func (this *mongo_serialize[T]) UpdateStatus2Init(t itask.ITask) error {
	_, err := this.col().UpdateByID(context.Background(),
		t.GetID(),
		bson.M{"$set": bson.M{"Status": taskstatus.Init}},
	)
	return err
}

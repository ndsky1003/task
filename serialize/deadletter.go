package serialize

import (
	"context"

	"go.mongodb.org/mongo-driver/bson"

	"github.com/ndsky1003/task/task"
	"github.com/ndsky1003/task/taskstatus"
)

// MarkDead 将任务标记为死信状态（MongoDB）。
func (this *mongo_serialize) MarkDead(t *task.Task) error {
	_, err := this.col().UpdateByID(context.Background(), t.ID,
		bson.M{"$set": bson.M{"Status": taskstatus.Dead}})
	return err
}

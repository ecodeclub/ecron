package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/ecodeclub/ecron/internal/storage"
	"github.com/ecodeclub/ecron/internal/task"
	"github.com/ecodeclub/eorm"
	"github.com/stretchr/testify/assert"
)

// 增删改查
func Test_StorageTaskCURD(t *testing.T) {
	s, err := NewMysqlStorage("root:@tcp(localhost:3306)/ecron")
	assert.Nil(t, err)

	// 添加任务
	addTask := &task.Task{Config: task.Config{
		Name:       "Origin Task",
		Cron:       "*/5 * * * * * *", // every 5s
		Type:       task.TypeHTTP,
		Parameters: `{"url": "http://www.baidu.com", "body": "{\"key\": \"value\"}", "timeout": 30}`,
	}}
	tId, err := s.Add(context.TODO(), addTask)
	assert.Nil(t, err)
	assert.NotEmpty(t, tId)

	getTask, err := s.Get(context.TODO(), tId)
	assert.Nil(t, err)
	addTask.TaskId = tId
	assert.Equal(t, addTask, getTask)

	// 更新任务
	updateTask := &task.Task{
		Config: task.Config{
			Name:       "Update Task",
			Cron:       "*/20 * * * * * *", // every 5s
			Type:       task.TypeHTTP,
			Parameters: `{"url": "http://www.sina.com", "body": "{\"key\": \"value\"}", "timeout": 30}`,
		},
		TaskId: tId,
	}
	err = s.Update(context.TODO(), updateTask)
	assert.Nil(t, err)
	getUpdateTask, err := s.Get(context.TODO(), tId)
	assert.Nil(t, err)
	addTask.TaskId = tId
	assert.Equal(t, updateTask, getUpdateTask)

	// 删除任务
	err = s.Delete(context.TODO(), tId)
	assert.Nil(t, err)

	getDelTask, err := s.Get(context.TODO(), tId)
	assert.Nil(t, err)
	assert.Nil(t, getDelTask)

	err = s.Stop(context.TODO())
	assert.Nil(t, err)

	_ = eorm.NewDeleter[StorageInfo](s.db).From(&StorageInfo{}).Where(eorm.C("Id").EQ(s.storageId)).Exec(context.TODO())
}

// 单个storage抢占
func TestStorage_SinglePreempt(t *testing.T) {
	var (
		st *Storage
		db *eorm.DB
	)

	db, err := eorm.Open("mysql", "root:@tcp(localhost:3306)/ecron")
	assert.Nil(t, err)

	testCases := []struct {
		name                 string
		wantType             string
		before               func()
		after                func()
		wantOccupierPayload  int64
		wantCandidatePayload int64
		wantCandidateId      int64
	}{
		{
			name: "抢占处于创建的状态的任务",
			before: func() {
				st, _ = newMysqlStorage(db)
				for i := 0; i < 2; i++ {
					_ = eorm.NewInserter[TaskInfo](db).Values(&TaskInfo{
						Name:            "test task",
						Cron:            "*/5 * * * * * *",
						SchedulerStatus: storage.EventTypeCreated,
						CreateTime:      time.Now().Unix(),
						UpdateTime:      time.Now().Unix(),
					}).Exec(context.TODO()).Err()
				}
			},
			after: func() {
				_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Exec(context.TODO())
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Exec(context.TODO())
			},
			wantType:            storage.EventTypePreempted,
			wantOccupierPayload: 2,
		},
		{
			name: "抢占由于负载问题被放弃的任务",
			before: func() {
				st, _ = newMysqlStorage(db)
				for i := 0; i < 2; i++ {
					_ = eorm.NewInserter[TaskInfo](db).Values(&TaskInfo{
						Name:            "test task",
						SchedulerStatus: storage.EventTypeDiscarded,
						CandidateId:     st.storageId,
						CreateTime:      time.Now().Unix(),
						UpdateTime:      time.Now().Unix(),
					}).Exec(context.TODO()).Err()
				}
			},
			after: func() {
				_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Exec(context.TODO())
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Exec(context.TODO())
			},
			wantType:            storage.EventTypePreempted,
			wantOccupierPayload: 2,
		},
		{
			name: "抢占处于抢占状态但是在续约周期内没有续约的任务",
			before: func() {
				retry := &storage.RefreshIntervalRetry{Interval: time.Second, Max: 3}
				st, _ = newMysqlStorage(db, WithRefreshRetry(retry))
				for i := 0; i < 2; i++ {
					_ = eorm.NewInserter[TaskInfo](db).Values(&TaskInfo{
						Name:            "test task",
						SchedulerStatus: storage.EventTypePreempted,
						OccupierId:      st.storageId,
						// 模拟续约超时
						UpdateTime: time.Now().Unix() - (retry.GetMaxRetry()+1000)*retry.Interval.Milliseconds(),
					}).Exec(context.TODO()).Err()
				}
			},
			after: func() {
				_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Exec(context.TODO())
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Exec(context.TODO())
			},
			wantType:            storage.EventTypePreempted,
			wantOccupierPayload: 2,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			go st.preempted(context.TODO())
			taskIds := make([]int64, 0, 2)
		LOOP:
			for {
				select {
				case e := <-st.events:
					taskIds = append(taskIds, e.Task.TaskId)
					if len(taskIds) == 2 {
						break LOOP
					}
				}
			}
			for _, tId := range taskIds {
				taskDb := getDbTask(db, tId)
				occupierPayload := st.getOccupierPayload(context.TODO(), st.storageId)
				assert.Equal(t, tc.wantOccupierPayload, occupierPayload)
				assert.Equal(t, st.storageId, taskDb.OccupierId)
				assert.Equal(t, tc.wantType, taskDb.SchedulerStatus)
				assert.Equal(t, tc.wantCandidateId, taskDb.CandidateId)
			}
			tc.after()
		})
	}
}

// 任务续约。eorm.openDB是私有，暂无法覆盖所有场景

func TestStorage_Refresh(t *testing.T) {
	db, err := eorm.Open("mysql", "root:@tcp(localhost:3306)/ecron")
	assert.Nil(t, err)

	var taskId int64
	s, _ := newMysqlStorage(db, WithRefreshInterval(5*time.Second))
	testCases := []struct {
		name       string
		before     func()
		after      func()
		wantEpoch  int64
		wantRetry  int64
		wantStatus string
	}{
		{
			name: "一次续约即成功",
			before: func() {
				taskId, _ = eorm.NewInserter[TaskInfo](db).Values(&TaskInfo{
					Name:            "test task",
					SchedulerStatus: storage.EventTypePreempted,
					OccupierId:      s.storageId,
					CreateTime:      time.Now().Unix(),
					UpdateTime:      time.Now().Unix(),
				}).Exec(context.TODO()).LastInsertId()
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Exec(context.TODO())
			},
			wantEpoch: 1,
		},
		{
			name: "续约时放弃任务",
			before: func() {
				taskId, _ = eorm.NewInserter[TaskInfo](db).Values(&TaskInfo{
					Name:            "test task",
					SchedulerStatus: storage.EventTypePreempted,
					OccupierId:      s.storageId,
					CandidateId:     123,
					CreateTime:      time.Now().Unix(),
					UpdateTime:      time.Now().Unix(),
				}).Exec(context.TODO()).LastInsertId()
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Exec(context.TODO())
			},
			wantEpoch:  1,
			wantStatus: storage.EventTypeDiscarded,
		},
		//{
		//	name: "多次续约才成功",
		//},
		//{
		//	name: "续约都失败",
		//},
		//{
		//	name: "续约超时",
		//},
		//{
		//	name: "续约时，任务状态由抢占变为非抢占状态",
		//},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			tskBefore := getDbTask(db, taskId)
			assert.Equal(t, taskId, tskBefore.Id)
			s.refresh(context.TODO(), tskBefore.Id, tskBefore.Epoch, tskBefore.CandidateId)
			tskAfter := getDbTask(db, taskId)
			assert.Equal(t, tc.wantEpoch, tskAfter.Epoch)
			assert.Equal(t, tc.wantRetry, s.refreshRetry.GetCntRetry())
			if tc.wantStatus != "" {
				assert.Equal(t, tc.wantStatus, tskAfter.SchedulerStatus)
			}
			tc.after()
		})
	}
	_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Exec(context.TODO())
}

// 任务负载更新(任务均衡) 。确保当前storage，是否按照需求要更新到遍历的任务中

func TestStorage_Lookup(t *testing.T) {
	db, err := eorm.Open("mysql", "root:@tcp(localhost:3306)/ecron")
	assert.Nil(t, err)
	s1, _ := newMysqlStorage(db) // task占有者storage
	s2, _ := newMysqlStorage(db)
	s, _ := newMysqlStorage(db) // 当前storage

	taskId, _ := eorm.NewInserter[TaskInfo](db).Values(&TaskInfo{
		SchedulerStatus: storage.EventTypePreempted,
		OccupierId:      s.storageId,
		CreateTime:      time.Now().Unix(),
		UpdateTime:      time.Now().Unix(),
	}).Exec(context.TODO()).LastInsertId()

	testCases := []struct {
		name            string
		before          func()
		after           func()
		wantOccupierId  int64
		wantCandidateId int64
	}{
		{
			name: "占有者就是当前storage(跳过本次balance)",
			before: func() {
				_ = eorm.NewUpdater[TaskInfo](db).Update(&TaskInfo{
					CandidateId: 0,
					OccupierId:  s.storageId,
					UpdateTime:  time.Now().Unix(),
				}).Set(eorm.Columns("CandidateId", "OccupierId", "UpdateTime")).Where(eorm.C("Id").EQ(taskId)).Exec(context.TODO()).Err()
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Where(eorm.C("Id").NEQ(taskId)).Exec(context.TODO())
			},
			wantCandidateId: 0, // 无候选者
			wantOccupierId:  s.storageId,
		},
		{
			name: "task无候选者,当前节点比占有节点负载大，不更新候选者",
			before: func() {
				_ = eorm.NewUpdater[TaskInfo](db).Update(&TaskInfo{
					CandidateId: 0,
					OccupierId:  s1.storageId,
					UpdateTime:  time.Now().Unix(),
				}).Set(eorm.Columns("CandidateId", "OccupierId", "UpdateTime")).Where(eorm.C("Id").EQ(taskId)).Exec(context.TODO()).Err()
				_ = addOccupierTaskN(s, 4)
				_ = addCandidateTaskN(s, 4)
				_ = addOccupierTaskN(s1, 1)
				_ = addOccupierTaskN(s2, 3)
				_ = addCandidateTaskN(s2, 1)
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Where(eorm.C("Id").NEQ(taskId)).Exec(context.TODO())
			},
			wantCandidateId: 0, // 无候选者
			wantOccupierId:  s1.storageId,
		},
		{
			name: "task无候选者,待选节点比占有节点负载小, 小的不到2*s.n",
			before: func() {
				_ = eorm.NewUpdater[TaskInfo](db).Update(&TaskInfo{
					CandidateId: 0,
					OccupierId:  s1.storageId,
					UpdateTime:  time.Now().Unix(),
				}).Set(eorm.Columns("CandidateId", "OccupierId", "OccupierId", "UpdateTime")).Where(eorm.C("Id").EQ(taskId)).Exec(context.TODO()).Err()
				_ = addOccupierTaskN(s, 1)
				_ = addCandidateTaskN(s, 3)
				_ = addOccupierTaskN(s1, 8)
				_ = addOccupierTaskN(s2, 1)
				_ = addCandidateTaskN(s2, 1)
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Where(eorm.C("Id").NEQ(taskId)).Exec(context.TODO())
			},
			wantCandidateId: 0, // 无候选者
			wantOccupierId:  s1.storageId,
		},
		{
			name: "task无候选者,待选节点比占有节点负载小, 差值大于2*s.n",
			before: func() {
				_ = eorm.NewUpdater[TaskInfo](db).Update(&TaskInfo{
					CandidateId: 0,
					OccupierId:  s1.storageId,
					UpdateTime:  time.Now().Unix(),
				}).Set(eorm.Columns("CandidateId", "OccupierId", "OccupierId", "UpdateTime")).Where(eorm.C("Id").EQ(taskId)).Exec(context.TODO()).Err()
				_ = addOccupierTaskN(s1, 9)
				_ = addOccupierTaskN(s2, 1)
				_ = addCandidateTaskN(s2, 1)
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Where(eorm.C("Id").NEQ(taskId)).Exec(context.TODO())
			},
			wantCandidateId: s.storageId,
			wantOccupierId:  s1.storageId,
		},
		{
			name: "task有候选者,待选节点比占有节点负载小, 小的不到2*s.n",
			before: func() {
				_ = eorm.NewUpdater[TaskInfo](db).Update(&TaskInfo{
					CandidateId: s2.storageId,
					OccupierId:  s1.storageId,
					UpdateTime:  time.Now().Unix(),
				}).Set(eorm.Columns("CandidateId", "OccupierId", "OccupierId", "UpdateTime")).Where(eorm.C("Id").EQ(taskId)).Exec(context.TODO()).Err()
				_ = addOccupierTaskN(s1, 8)
				_ = addOccupierTaskN(s2, 1)
				_ = addCandidateTaskN(s2, 1)
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Where(eorm.C("Id").NEQ(taskId)).Exec(context.TODO())
			},
			wantCandidateId: s2.storageId,
			wantOccupierId:  s1.storageId,
		},
		{
			name: "task有候选者,待选节点比占有节点负载小, 差值大于2*s.n",
			before: func() {
				_ = eorm.NewUpdater[TaskInfo](db).Update(&TaskInfo{
					CandidateId: s2.storageId,
					OccupierId:  s1.storageId,
					UpdateTime:  time.Now().Unix(),
				}).Set(eorm.Columns("CandidateId", "OccupierId", "OccupierId", "UpdateTime")).Where(eorm.C("Id").EQ(taskId)).Exec(context.TODO()).Err()
				_ = addOccupierTaskN(s1, 3)
				// 给s作为候选者，s1作为占有者增加2个task
				_ = addOccupierWithCandidateTaskN(s1, s.storageId, 2)
				_ = addOccupierWithCandidateTaskN(s1, s2.storageId, 4)
				_ = addOccupierTaskN(s2, 3)
				_ = addCandidateTaskN(s2, 1)
			},
			after: func() {
				_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Where(eorm.C("Id").NEQ(taskId)).Exec(context.TODO())
			},
			wantCandidateId: s.storageId,
			wantOccupierId:  s1.storageId,
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tc.before()
			s.lookup(context.TODO())
			tsk := getDbTask(db, taskId)
			assert.Equal(t, tc.wantOccupierId, tsk.OccupierId)
			assert.Equal(t, tc.wantCandidateId, tsk.CandidateId)
			tc.after()
		})
	}
	_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Where(eorm.C("Id").EQ(s.storageId)).Exec(context.TODO())
	_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Where(eorm.C("Id").EQ(s1.storageId)).Exec(context.TODO())
	_ = eorm.NewDeleter[StorageInfo](db).From(&StorageInfo{}).Where(eorm.C("Id").EQ(s2.storageId)).Exec(context.TODO())
	_ = eorm.NewDeleter[TaskInfo](db).From(&TaskInfo{}).Exec(context.TODO())
}

func getDbTask(db *eorm.DB, taskId int64) *TaskInfo {
	ts, _ := eorm.NewSelector[TaskInfo](db).From(eorm.TableOf(&TaskInfo{}, "t1")).
		Where(eorm.C("Id").EQ(taskId)).
		Get(context.TODO())
	return ts
}

func addOccupierTaskN(s *Storage, n int) []int64 {
	taskIds := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		tid1, _ := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
			Name:            "test task",
			SchedulerStatus: storage.EventTypePreempted,
			OccupierId:      s.storageId,
			CreateTime:      time.Now().Unix(),
			UpdateTime:      time.Now().Unix(),
		}).Exec(context.TODO()).LastInsertId()
		taskIds = append(taskIds, tid1)
	}
	return taskIds
}

func addCandidateTaskN(s *Storage, n int) []int64 {
	taskIds := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		tid1, _ := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
			Name:            "test task",
			SchedulerStatus: storage.EventTypePreempted,
			CandidateId:     s.storageId,
			CreateTime:      time.Now().Unix(),
			UpdateTime:      time.Now().Unix(),
		}).Exec(context.TODO()).LastInsertId()
		taskIds = append(taskIds, tid1)
	}
	return taskIds
}

func addCandidateWithOccupierTaskN(s *Storage, occupierId int64, n int) []int64 {
	taskIds := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		tid1, _ := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
			Name:            "test task",
			SchedulerStatus: storage.EventTypePreempted,
			CandidateId:     s.storageId,
			OccupierId:      occupierId,
			CreateTime:      time.Now().Unix(),
			UpdateTime:      time.Now().Unix(),
		}).Exec(context.TODO()).LastInsertId()
		taskIds = append(taskIds, tid1)
	}
	return taskIds
}

func addOccupierWithCandidateTaskN(s *Storage, candidateId int64, n int) []int64 {
	taskIds := make([]int64, 0, n)
	for i := 0; i < n; i++ {
		tid1, _ := eorm.NewInserter[TaskInfo](s.db).Values(&TaskInfo{
			Name:            "test task",
			SchedulerStatus: storage.EventTypePreempted,
			CandidateId:     candidateId,
			OccupierId:      s.storageId,
			CreateTime:      time.Now().Unix(),
			UpdateTime:      time.Now().Unix(),
		}).Exec(context.TODO()).LastInsertId()
		taskIds = append(taskIds, tid1)
	}
	return taskIds
}

package schedule

import (
	"container/heap"
	"errors"
	"github.com/panjf2000/ants/v2"
	"math"
	"sync/atomic"
	"time"
)

var (
	// ErrScheduleShutdown 延迟任务调度器已关闭错误
	ErrScheduleShutdown = errors.New("schedule: schedule is already in shutdown")
)

const invalidTaskId = 0

type TaskId uint32
type OriginalTaskId uint32

// Schedule 延迟调度的结构体，提供延迟调度任务的全部方法
// 通过NewSchedule方法创建Schedule，通过Schedule、ScheduleOne方法添加延迟调度任务，通过CancelTask方法取消任务，通过Shutdown停止延迟任务
type Schedule struct {
	//任务堆，按时间排序
	taskHeap taskHeap
	//可执行的任务Map，key是当前的任务id，value是任务的第一次原始id，用于优化取消任务时需要遍历堆去删除
	executeTaskIdMap map[TaskId]OriginalTaskId
	//任务id的Map，key是任务的第一次原始id，value是当前的任务id，用于优化取消任务时需要遍历堆去删除
	originalTaskIdMap map[OriginalTaskId]TaskId
	//调度器是否运行中
	running atomic.Bool
	//下一个任务id
	nextTaskId atomic.Uint32
	//任务运行池
	pool *ants.Pool
	//添加任务Chan
	addTaskChan chan *Task
	//删除任务Chan
	stopTaskChan chan struct{}
	//取消任务Chan
	cancelTaskChan chan OriginalTaskId
}

// NewSchedule 构建一个Schedule
// workerNum 工作的协程数量，options ants协程池的配置，除了WithMaxBlockingTasks不能配置，别的都可以，具体参考：https://github.com/panjf2000/ants
func NewSchedule(workerNum int, options ...ants.Option) (*Schedule, error) {
	//延迟任务的最大任务数量必须不限制
	options = append(options, ants.WithMaxBlockingTasks(0))
	//创建一个协程池
	pool, err := ants.NewPool(workerNum)
	if err != nil {
		return nil, err
	}
	//创建一个延迟调度结构体
	s := &Schedule{
		taskHeap:          make(taskHeap, 0),
		executeTaskIdMap:  make(map[TaskId]OriginalTaskId),
		originalTaskIdMap: make(map[OriginalTaskId]TaskId),
		running:           atomic.Bool{},
		nextTaskId:        atomic.Uint32{},
		pool:              pool,
		addTaskChan:       make(chan *Task),
		stopTaskChan:      make(chan struct{}),
		cancelTaskChan:    make(chan OriginalTaskId),
	}
	//启动调度 会开启一个协程去将即将要调度的任务添加到协程池中运行
	s.start()
	return s, nil
}

// ScheduleOne 添加延迟调度任务，只调度一次
// job 执行的方法 duration 周期间隔，如果是负数立马执行，如果是负数立马且只执行一次
func (s *Schedule) ScheduleOne(job func(), duration time.Duration) (uint32, error) {
	return s.doSchedule(job, duration, true)
}

// Schedule 添加延迟调度任务，重复调度
// job 执行的方法 duration 周期间隔，如果是负数立马且只执行一次
func (s *Schedule) Schedule(job func(), duration time.Duration) (uint32, error) {
	return s.doSchedule(job, duration, false)
}

// doSchedule 添加延迟调度任务的具体实现
func (s *Schedule) doSchedule(job func(), duration time.Duration, onlyOne bool) (uint32, error) {
	if s.running.Load() {
		//如果是负数 只执行一次
		if duration <= 0 {
			onlyOne = true
		}
		nextTaskId := s.getNextTaskId()
		task := new(Task)
		task.job = job
		task.executeTime = time.Now().Add(duration)
		task.onlyOne = onlyOne
		task.duration = duration
		task.originalId = OriginalTaskId(nextTaskId)
		task.id = TaskId(nextTaskId)
		s.addTaskChan <- task
		return uint32(task.originalId), nil
	} else {
		return invalidTaskId, ErrScheduleShutdown
	}
}

// CancelTask 取消延迟调度任务
// taskId 任务id
func (s *Schedule) CancelTask(taskId uint32) {
	if s.running.Load() {
		if taskId != invalidTaskId {
			s.cancelTaskChan <- OriginalTaskId(taskId)
		}
	}
}

// Shutdown 结束延迟任务调度
func (s *Schedule) Shutdown() {
	//通过cas设值
	if s.running.CompareAndSwap(true, false) {
		s.stopTaskChan <- struct{}{}
	}
}

// IsShutdown 延迟任务调度是否关闭
func (s *Schedule) IsShutdown() bool {
	return !s.running.Load()
}

// start 启动延迟任务调度
func (s *Schedule) start() {
	s.running.Store(true)
	go func() {
		for {
			now := time.Now()
			var timer *time.Timer
			//如果没有任务提交，睡眠等待任务
			if s.taskHeap.Len() == 0 {
				timer = time.NewTimer(math.MaxUint16 * time.Hour)
			} else {
				//查看第一个要执行的任务是否是被取消的
				task := s.taskHeap.Peek()
				_, ok := s.executeTaskIdMap[task.id]
				if !ok {
					//是被取消的任务，移除后continue
					heap.Pop(&s.taskHeap)
					continue
				} else {
					//设置执行间隔
					timer = time.NewTimer(task.executeTime.Sub(now))
				}
			}
			select {
			case <-timer.C:
				//到达第一个任务执行时间
				task := heap.Pop(&s.taskHeap).(*Task)
				//提交到线程池执行，返回的error不需要处理，因为任务池是无限大
				_ = s.pool.Submit(task.job)
				//单次执行则删除，多次执行，则更新
				if task.onlyOne {
					s.removeTask(task.originalId, task.id)
				} else {
					s.updateTask(task)
				}
			case originalTaskId := <-s.cancelTaskChan:
				timer.Stop()
				//如果取消的任务id在待执行任务列表中，则删除任务
				if taskId, ok := s.originalTaskIdMap[originalTaskId]; ok {
					s.removeTask(originalTaskId, taskId)
				}
			case task := <-s.addTaskChan:
				timer.Stop()
				//添加任务
				s.addTask(task)
			case <-s.stopTaskChan:
				timer.Stop()
				//关闭资源
				s.close()
				return
			}
		}
	}()
}

// updateTask 更新延迟调度任务
func (s *Schedule) updateTask(executedTask *Task) {
	//拷贝 并设置新的执行时间和ID
	task := *executedTask
	task.executeTime = time.Now().Add(task.duration)
	nextTaskId := s.getNextTaskId()
	task.id = TaskId(nextTaskId)
	//把已执行的任务删除
	s.removeTask(invalidTaskId, executedTask.id)
	//添加新的任务
	s.addTask(&task)
}

// removeTask 移除任务
func (s *Schedule) removeTask(originalTaskId OriginalTaskId, taskId TaskId) {
	//如果原始的任务ID不为空，则为使用者取消的，从任务Map中也删除
	if originalTaskId != invalidTaskId {
		delete(s.originalTaskIdMap, originalTaskId)
	}
	delete(s.executeTaskIdMap, taskId)
}

// addTask 添加任务
func (s *Schedule) addTask(task *Task) {
	s.originalTaskIdMap[task.originalId] = task.id
	s.executeTaskIdMap[task.id] = task.originalId
	heap.Push(&s.taskHeap, task)
}

// getNextTaskId 获取下一个任务id
func (s *Schedule) getNextTaskId() uint32 {
	taskId := s.nextTaskId.Add(1)
	if taskId == invalidTaskId {
		taskId = s.nextTaskId.Add(1)
	}
	return taskId
}

// close 关闭Schedule资源和协程池的资源
func (s *Schedule) close() {
	//关闭所有资源并设置为 nil help gc
	s.taskHeap = nil
	s.executeTaskIdMap = nil
	s.originalTaskIdMap = nil
	s.pool.Release()
	s.pool = nil
	close(s.addTaskChan)
	close(s.cancelTaskChan)
	close(s.stopTaskChan)
	s.addTaskChan = nil
	s.cancelTaskChan = nil
	s.stopTaskChan = nil
}

// Task 调度任务结构体，是一个调度任务的实体信息
type Task struct {
	// 原始id，用于Schedule本身的删除使用，用两层Map的方式优化数组删除的O(n)时间复杂度
	originalId OriginalTaskId
	// 任务id
	id TaskId
	// 执行的时间，每次执行完，如果重复调度就重新计算
	executeTime time.Time
	// 周期间隔
	duration time.Duration
	// 执行的任务
	job func()
	// 是否只执行一次
	onlyOne bool
}

// 任务的堆，使用队只需要在添加的时候进行排序，堆顶是最先要执行的任务
type taskHeap []*Task

// 下面都是堆接口的实现

func (t *taskHeap) Len() int {
	return len(*t)
}
func (t *taskHeap) Less(i, j int) bool {
	return (*t)[i].executeTime.Before((*t)[j].executeTime)
}

func (t *taskHeap) Swap(i, j int) {
	(*t)[i], (*t)[j] = (*t)[j], (*t)[i]
}

func (t *taskHeap) Push(x interface{}) {
	*t = append(*t, x.(*Task))
}

func (t *taskHeap) Pop() interface{} {
	old := *t
	n := len(old)
	x := old[n-1]
	old[n-1] = nil
	*t = old[:n-1]
	return x
}

// Peek 查看堆顶元素，非堆接口的实现
func (t *taskHeap) Peek() *Task {
	return (*t)[0]
}

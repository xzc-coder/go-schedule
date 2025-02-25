package schedule

import (
	"container/heap"
	"errors"
	"github.com/panjf2000/ants/v2"
	"math"
	"sync"
	"sync/atomic"
	"time"
)

var (
	// ErrScheduleShutdown 延迟任务调度器已关闭错误
	ErrScheduleShutdown = errors.New("schedule: schedule is already in shutdown")
)

const InvalidTaskId = 0

// Schedule 延迟调度的结构体，提供延迟调度任务的全部方法
// 通过NewSchedule方法创建Schedule，通过Schedule、ScheduleOne方法添加延迟调度任务，通过CancelTask方法取消任务，通过Shutdown停止延迟任务
type Schedule struct {
	//任务堆，按时间排序
	taskHeap taskHeap
	//可执行的任务Map，key是当前的任务id，value是任务的第一次原始id，用于优化取消任务时需要遍历堆去删除
	executeTaskMap map[TaskId]TaskId
	//任务id的Map，key是任务的第一次原始id，value是当前的任务id，用于优化取消任务时需要遍历堆去删除
	taskIdMap map[TaskId]TaskId
	//锁 保证并发安全
	mux sync.Mutex
	//调度器是否运行中
	running bool
	//任务运行池
	pool *ants.Pool
	//下一个任务id
	nextTaskId TaskId
	//添加任务Chan
	addTaskChan chan *Task
	//删除任务Chan
	stopTaskChan chan struct{}
	//取消任务Chan
	cancelTaskChan chan TaskId
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
		taskHeap:       make(taskHeap, 0),
		executeTaskMap: make(map[TaskId]TaskId),
		taskIdMap:      make(map[TaskId]TaskId),
		running:        true,
		nextTaskId:     0,
		mux:            sync.Mutex{},
		pool:           pool,
		addTaskChan:    make(chan *Task),
		stopTaskChan:   make(chan struct{}),
		cancelTaskChan: make(chan TaskId),
	}
	//启动调度 会开启一个协程去将即将要调度的任务添加到协程池中运行
	s.start()
	return s, nil
}

// ScheduleOne 添加延迟调度任务，只调度一次
// job 执行的方法 duration 周期间隔，如果是负数立马执行，如果是负数立马且只执行一次
func (s *Schedule) ScheduleOne(job func(), duration time.Duration) (TaskId, error) {
	return s.doSchedule(job, duration, true)
}

// Schedule 添加延迟调度任务，重复调度
// job 执行的方法 duration 周期间隔，如果是负数立马且只执行一次
func (s *Schedule) Schedule(job func(), duration time.Duration) (TaskId, error) {
	return s.doSchedule(job, duration, false)
}

// doSchedule 添加延迟调度任务的具体实现
func (s *Schedule) doSchedule(job func(), duration time.Duration, onlyOne bool) (TaskId, error) {
	s.mux.Lock()
	defer s.mux.Unlock()
	//如果是负数 只执行一次
	if duration <= 0 {
		onlyOne = true
	}
	if s.running {
		task := &Task{
			job:         job,
			executeTime: time.Now().Add(duration),
			onlyOne:     onlyOne,
			duration:    duration,
		}
		//设置任务id并添加到任务堆中
		s.setTaskId(task, true)
		s.addTaskChan <- task
		return task.originalId, nil
	} else {
		return InvalidTaskId, ErrScheduleShutdown
	}
}

// CancelTask 取消延迟调度任务
// taskId 任务id
func (s *Schedule) CancelTask(taskId TaskId) {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.running {
		s.cancelTaskChan <- taskId
	}
}

// Shutdown 结束延迟任务调度
func (s *Schedule) Shutdown() {
	s.mux.Lock()
	defer s.mux.Unlock()
	if s.running {
		s.running = false
		s.stopTaskChan <- struct{}{}
	}
}

// IsShutdown 延迟任务调度是否关闭
func (s *Schedule) IsShutdown() bool {
	s.mux.Lock()
	defer s.mux.Unlock()
	return !s.running
}

// start 启动延迟任务调度
func (s *Schedule) start() {
	go func() {
		for {
			now := time.Now()
			var timer *time.Timer
			//如果没有任务提交，睡眠等待任务
			if s.taskHeap.Len() == 0 {
				timer = time.NewTimer(math.MaxUint16 * time.Hour)
			} else {
				//查看第一个要执行的任务是否是被取消的
				task := s.taskHeap.Peek().(*Task)
				_, ok := s.executeTaskMap[task.id]
				if !ok {
					//是被取消的任务，移除后continue
					s.taskHeap.Pop()
					continue
				} else {
					//设置执行间隔
					timer = time.NewTimer(task.executeTime.Sub(now))
				}
			}
			select {
			case <-timer.C:
				//到达第一个任务执行时间
				task := s.taskHeap.Pop().(*Task)
				//提交到线程池执行，返回的error不需要处理，因为任务池是无限大
				_ = s.pool.Submit(task.job)
				//单次执行则删除，多次执行，则更新
				if task.onlyOne {
					s.removeTask(&task.originalId, task.id)
				} else {
					s.updateTask(task)
				}
			case originalTaskId := <-s.cancelTaskChan:
				timer.Stop()
				//如果取消的任务id在待执行任务列表中，则删除任务
				if taskId, ok := s.taskIdMap[originalTaskId]; ok {
					s.removeTask(&originalTaskId, taskId)
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
	//设置任务ID
	s.setTaskId(&task, false)
	//把已执行的任务删除
	s.removeTask(nil, executedTask.id)
	//添加新的任务
	s.addTask(&task)
}

// removeTask 移除任务
func (s *Schedule) removeTask(originalId *TaskId, taskId TaskId) {
	//如果原始的任务ID不为空，则为使用者取消的，从任务Map中也删除
	if originalId != nil {
		delete(s.taskIdMap, *originalId)
	}
	delete(s.executeTaskMap, taskId)
}

// addTask 添加任务
func (s *Schedule) addTask(task *Task) {
	s.taskIdMap[task.originalId] = task.id
	s.executeTaskMap[task.id] = task.originalId
	heap.Push(&s.taskHeap, task)
}

// setTaskId 设置任务id
func (s *Schedule) setTaskId(task *Task, addOriginalId bool) {
	//为了兼容低版本，使用旧版本的CAS去设值，保证可见性
	atomic.AddInt32((*int32)(&s.nextTaskId), 1)
	if atomic.LoadInt32((*int32)(&s.nextTaskId)) == InvalidTaskId {
		atomic.AddInt32((*int32)(&s.nextTaskId), 1)
	}
	task.id = TaskId(atomic.LoadInt32((*int32)(&s.nextTaskId)))
	if addOriginalId {
		task.originalId = task.id
	}
}

// close 关闭Schedule资源和协程池的资源
func (s *Schedule) close() {
	s.taskHeap = nil
	s.taskIdMap = nil
	s.executeTaskMap = nil
	s.pool.Release()
	close(s.addTaskChan)
	close(s.cancelTaskChan)
	close(s.stopTaskChan)
	s.pool = nil
	s.addTaskChan = nil
	s.cancelTaskChan = nil
	s.stopTaskChan = nil
}

// TaskId 任务ID
type TaskId int32

// Task 调度任务结构体，是一个调度任务的实体信息
type Task struct {
	// 原始id，用于Schedule本身的删除使用，用两层Map的方式优化数组删除的O(n)时间复杂度
	originalId TaskId
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
	return (*t)[i].executeTime.After((*t)[j].executeTime)
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
	*t = old[:n-1]
	return x
}

// Peek 查看堆顶元素，非堆接口的实现
func (t *taskHeap) Peek() interface{} {
	return (*t)[len(*t)-1]
}

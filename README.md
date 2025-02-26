# go-schedule

基于Go协程池ants和Go并发编程实现的延迟任务调度器

## 用途

主要是用于高并发及大量定时任务要处理的情况，如果使用Go协程来实现每次延迟任务的调度，那么数量极大的goroutine将会占用内存，导致性能下降，使用协程池实现延迟任务的调度，会改善该情况。
如在物联网设备中，当连接数量达到几十万时，如果使用goroutine来处理心跳或者活跃检测，频繁的创建销毁goroutine会影响性能。

## 特色

在常见的cron等开源框架中使用的是数组存储待调度的任务，每次循环时都要排序，并且要删除某个任务则时间复杂度是O(n)。

本文通过使用堆及双重Map优化存储待调度的任务，使得添加任务时间复杂度为O(log n)，获取任务时间复杂度为O(1)，删除时间复杂度为O(1)。

调度器并不会真正的删除取消任务，当取消任务达到执行时间时，会直接continue，是为了提高删除效率，如果要删除取消任务，那么删除的时间复杂度为O(log n)，当有极大量任务时，会占用一些内存，通过空间换时间来提高删除效率，如果要使用该调度器，请参考我的CSDN博客，最底部有该版本的实现：
https://blog.csdn.net/m0_37914588/article/details/145864055


## API

### 创建

```
NewSchedule(workerNum int, options ...ants.Option) (*Schedule, error) 

//创建协程数是1的延迟任务调度器
s, _ := NewSchedule(1)
```

创建一个延迟调度任务器，workerNum是协程数量，options是ants协程池的配置，除了WithMaxBlockingTasks不能配置，别的都可以，具体参考：https://github.com/panjf2000/ants

### 调度一次

```
func (s *Schedule) ScheduleOne(job func(), duration time.Duration) (TaskId, error) 

//1秒后打印一次时间
taskId, _ := s.ScheduleOne(func() {
		fmt.Println(time.Now())
}, time.Second)
```

### 重复调度

```
func (s *Schedule) Schedule(job func(), duration time.Duration) (TaskId, error) 

//每隔一秒打印一次时间
taskId, _ := s.Schedule(func() {
		fmt.Println(time.Now())
}, time.Second)
```

### 取消调度

```
func (s *Schedule) Schedule(job func(), duration time.Duration) (TaskId, error) 

//每隔一秒打印一次时间
taskId, _ := s.Schedule(func() {
		fmt.Println(time.Now())
}, time.Second)
//休眠3秒后，取消调度
time.Sleep(3 * time.Second)
s.CancelTask(taskId)
```

### 停止调度

```
func (s *Schedule) Schedule(job func(), duration time.Duration) (TaskId, error) 

//每隔一秒打印一次时间
taskId, _ := s.Schedule(func() {
		fmt.Println(time.Now())
}, time.Second)
//休眠3秒后，停用延迟任务调度器
time.Sleep(3 * time.Second)
s.Shutdown()
```


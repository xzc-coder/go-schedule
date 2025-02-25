package schedule

import (
	"github.com/stretchr/testify/require"
	"sync/atomic"
	"testing"
	"time"
)

var num int32 = 0

func TestScheduleOne(t *testing.T) {
	s, _ := NewSchedule(1)
	_, _ = s.ScheduleOne(func() {
		atomic.AddInt32(&num, 1)
	}, 100*time.Millisecond)
	time.Sleep(300 * time.Millisecond)
	require.EqualValues(t, 1, atomic.LoadInt32(&num), "number value error")
}

func TestSchedule(t *testing.T) {
	s, _ := NewSchedule(1)
	taskId, _ := s.Schedule(func() {
		atomic.AddInt32(&num, 1)
	}, 100*time.Millisecond)
	time.Sleep(350 * time.Millisecond)
	s.CancelTask(taskId)
	require.EqualValues(t, 3, atomic.LoadInt32(&num), "number value error")
	require.EqualValues(t, false, s.IsShutdown(), "status value error")
}

func TestShutdown(t *testing.T) {
	s, _ := NewSchedule(1)
	_, _ = s.Schedule(func() {
		atomic.AddInt32(&num, 1)
	}, 100*time.Millisecond)
	time.Sleep(350 * time.Millisecond)
	s.Shutdown()
	require.EqualValues(t, 3, atomic.LoadInt32(&num), "number value error")
	require.EqualValues(t, true, s.IsShutdown(), "status value error")
}

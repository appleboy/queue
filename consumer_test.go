package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestQueueUsage(t *testing.T) {
	w := NewConsumer()
	assert.Equal(t, defaultQueueSize, w.Capacity())
	assert.Equal(t, 0, w.Usage())

	assert.NoError(t, w.Queue(&mockMessage{}))
	assert.Equal(t, 1, w.Usage())
}

func TestMaxCapacity(t *testing.T) {
	w := NewConsumer(WithQueueSize(2))
	assert.Equal(t, 2, w.Capacity())
	assert.Equal(t, 0, w.Usage())

	assert.NoError(t, w.Queue(&mockMessage{}))
	assert.Equal(t, 1, w.Usage())
	assert.NoError(t, w.Queue(&mockMessage{}))
	assert.Equal(t, 2, w.Usage())
	assert.Error(t, w.Queue(&mockMessage{}))
	assert.Equal(t, 2, w.Usage())

	err := w.Queue(&mockMessage{})
	assert.Equal(t, errMaxCapacity, err)
}

func TestCustomFuncAndWait(t *testing.T) {
	m := mockMessage{
		message: "foo",
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	assert.NoError(t, q.Queue(m))
	time.Sleep(600 * time.Millisecond)
	q.Shutdown()
	q.Wait()
	// you will see the execute time > 1000ms
}

func TestEnqueueJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		message: "foo",
	}
	w := NewConsumer()
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	q.Shutdown()
	// can't queue task after shutdown
	err = q.Queue(m)
	assert.Error(t, err)
	assert.Equal(t, ErrQueueShutdown, err)
	q.Wait()
}

func TestConsumerNumAfterShutdown(t *testing.T) {
	w := NewConsumer()
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 4, q.Workers())
	q.Shutdown()
	q.Wait()
	assert.Equal(t, 0, q.Workers())
	// show queue has been shutdown meesgae
	q.Start()
	q.Start()
	assert.Equal(t, 0, q.Workers())
}

func TestJobReachTimeout(t *testing.T) {
	m := mockMessage{
		message: "foo",
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Bytes()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.QueueWithTimeout(30*time.Millisecond, m))
	time.Sleep(50 * time.Millisecond)
	q.Shutdown()
	q.Wait()
}

func TestCancelJobAfterShutdown(t *testing.T) {
	m := mockMessage{
		message: "foo",
	}
	w := NewConsumer(
		WithLogger(NewEmptyLogger()),
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Bytes()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
				}
				time.Sleep(50 * time.Millisecond)
			}
		}),
	)
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.QueueWithTimeout(100*time.Millisecond, m))
	q.Shutdown()
	q.Wait()
}

func TestGoroutineLeak(t *testing.T) {
	m := mockMessage{
		message: "foo",
	}
	w := NewConsumer(
		WithLogger(NewEmptyLogger()),
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			for {
				select {
				case <-ctx.Done():
					log.Println("get data:", string(m.Bytes()))
					if errors.Is(ctx.Err(), context.Canceled) {
						log.Println("queue has been shutdown and cancel the job")
					} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
						log.Println("job deadline exceeded")
					}
					return nil
				default:
					log.Println("get data:", string(m.Bytes()))
					time.Sleep(50 * time.Millisecond)
					return nil
				}
			}
		}),
	)
	q, err := NewQueue(
		WithLogger(NewEmptyLogger()),
		WithWorker(w),
		WithWorkerCount(10),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	for i := 0; i < 500; i++ {
		m.message = fmt.Sprintf("foobar: %d", i+1)
		assert.NoError(t, q.Queue(m))
	}
	time.Sleep(2 * time.Second)
	q.Shutdown()
	q.Wait()
	fmt.Println("number of goroutines:", runtime.NumGoroutine())
}

func TestGoroutinePanic(t *testing.T) {
	m := mockMessage{
		message: "foo",
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			panic("missing something")
		}),
	)
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	time.Sleep(50 * time.Millisecond)
	q.Shutdown()
	q.Wait()
}

func TestHandleTimeout(t *testing.T) {
	job := Job{
		Timeout: 100 * time.Millisecond,
		Body:    []byte("foo"),
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}),
	)

	err := w.handle(job)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	job = Job{
		Timeout: 150 * time.Millisecond,
		Body:    []byte("foo"),
	}

	w = NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}),
	)

	done := make(chan error)
	go func() {
		done <- w.handle(job)
	}()

	assert.NoError(t, w.Shutdown())

	err = <-done
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestJobComplete(t *testing.T) {
	job := Job{
		Timeout: 100 * time.Millisecond,
		Body:    []byte("foo"),
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			return errors.New("job completed")
		}),
	)

	err := w.handle(job)
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)

	job = Job{
		Timeout: 250 * time.Millisecond,
		Body:    []byte("foo"),
	}

	w = NewConsumer(
		WithFn(func(ctx context.Context, m QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return errors.New("job completed")
		}),
	)

	done := make(chan error)
	go func() {
		done <- w.handle(job)
	}()

	assert.NoError(t, w.Shutdown())

	err = <-done
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)
}

func TestTaskJobComplete(t *testing.T) {
	job := Job{
		Timeout: 100 * time.Millisecond,
		Task: func(ctx context.Context) error {
			return errors.New("job completed")
		},
	}
	w := NewConsumer()

	err := w.handle(job)
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)

	job = Job{
		Timeout: 250 * time.Millisecond,
		Task: func(ctx context.Context) error {
			return nil
		},
	}

	w = NewConsumer()
	done := make(chan error)
	go func() {
		done <- w.handle(job)
	}()
	assert.NoError(t, w.Shutdown())
	err = <-done
	assert.NoError(t, err)

	// job timeout
	job = Job{
		Timeout: 50 * time.Millisecond,
		Task: func(ctx context.Context) error {
			time.Sleep(60 * time.Millisecond)
			return nil
		},
	}
	assert.Equal(t, context.DeadlineExceeded, w.handle(job))
}

func TestBusyWorkerCount(t *testing.T) {
	job := Job{
		Timeout: 200 * time.Millisecond,
		Task: func(ctx context.Context) error {
			time.Sleep(100 * time.Millisecond)
			return nil
		},
	}

	w := NewConsumer()

	assert.Equal(t, uint64(0), w.BusyWorkers())
	go func() {
		assert.NoError(t, w.handle(job))
	}()
	go func() {
		assert.NoError(t, w.handle(job))
	}()

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, uint64(2), w.BusyWorkers())
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, uint64(0), w.BusyWorkers())

	assert.NoError(t, w.Shutdown())
}

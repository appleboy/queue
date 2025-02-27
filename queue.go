package queue

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"

	"github.com/jpillora/backoff"
)

// ErrQueueShutdown the queue is released and closed.
var ErrQueueShutdown = errors.New("queue has been closed and released")

type (
	// A Queue is a message queue.
	Queue struct {
		sync.Mutex
		metric       *metric
		logger       Logger
		workerCount  int64
		routineGroup *routineGroup
		quit         chan struct{}
		ready        chan struct{}
		newTaskAdded chan struct{}
		worker       core.Worker
		stopOnce     sync.Once
		stopFlag     int32
		afterFn      func()
	}
)

// ErrMissingWorker missing define worker
var ErrMissingWorker = errors.New("missing worker module")

// NewQueue returns a Queue.
func NewQueue(opts ...Option) (*Queue, error) {
	o := NewOptions(opts...)
	q := &Queue{
		routineGroup: newRoutineGroup(),
		quit:         make(chan struct{}),
		ready:        make(chan struct{}, 1),
		newTaskAdded: make(chan struct{}),
		workerCount:  o.workerCount,
		logger:       o.logger,
		worker:       o.worker,
		metric:       &metric{},
		afterFn:      o.afterFn,
	}

	if q.worker == nil {
		return nil, ErrMissingWorker
	}

	return q, nil
}

// Start to enable all worker
func (q *Queue) Start() {
	q.Lock()
	count := q.workerCount
	q.Unlock()
	if count == 0 {
		return
	}
	q.routineGroup.Run(func() {
		q.start()
	})
}

// Shutdown stops all queues.
func (q *Queue) Shutdown() {
	if !atomic.CompareAndSwapInt32(&q.stopFlag, 0, 1) {
		return
	}

	q.stopOnce.Do(func() {
		if q.metric.BusyWorkers() > 0 {
			q.logger.Infof("shutdown all tasks: %d workers", q.metric.BusyWorkers())
		}

		if err := q.worker.Shutdown(); err != nil {
			q.logger.Error(err)
		}
		close(q.quit)
	})
}

// Release for graceful shutdown.
func (q *Queue) Release() {
	q.Shutdown()
	q.Wait()
}

// BusyWorkers returns the numbers of workers in the running process.
func (q *Queue) BusyWorkers() int64 {
	return q.metric.BusyWorkers()
}

// BusyWorkers returns the numbers of success tasks.
func (q *Queue) SuccessTasks() uint64 {
	return q.metric.SuccessTasks()
}

// BusyWorkers returns the numbers of failure tasks.
func (q *Queue) FailureTasks() uint64 {
	return q.metric.FailureTasks()
}

// BusyWorkers returns the numbers of submitted tasks.
func (q *Queue) SubmittedTasks() uint64 {
	return q.metric.SubmittedTasks()
}

// CompletedTasks returns the numbers of completed tasks.
func (q *Queue) CompletedTasks() uint64 {
	return q.metric.CompletedTasks()
}

// Wait all process
func (q *Queue) Wait() {
	q.routineGroup.Wait()
}

// Queue to queue single job with binary
func (q *Queue) Queue(message core.QueuedMessage, opts ...job.AllowOption) error {
	data := job.NewMessage(message, opts...)

	return q.queue(&data)
}

// QueueTask to queue single task
func (q *Queue) QueueTask(task job.TaskFunc, opts ...job.AllowOption) error {
	data := job.NewTask(task, opts...)
	return q.queue(&data)
}

func (q *Queue) queue(m *job.Message) error {
	if atomic.LoadInt32(&q.stopFlag) == 1 {
		return ErrQueueShutdown
	}

	if err := q.worker.Queue(m); err != nil {
		return err
	}

	q.metric.IncSubmittedTask()
	q.newTaskAdded <- struct{}{}

	return nil
}

func (q *Queue) work(task core.TaskMessage) {
	var err error
	// to handle panic cases from inside the worker
	// in such case, we start a new goroutine
	defer func() {
		q.metric.DecBusyWorker()
		e := recover()
		if e != nil {
			q.logger.Fatalf("panic error: %v", e)
		}
		q.schedule()

		// increase success or failure number
		if err == nil && e == nil {
			q.metric.IncSuccessTask()
		} else {
			q.metric.IncFailureTask()
		}
		if q.afterFn != nil {
			q.afterFn()
		}
	}()

	if err = q.run(task); err != nil {
		q.logger.Errorf("runtime error: %s", err.Error())
	}
}

func (q *Queue) run(task core.TaskMessage) error {
	switch t := task.(type) {
	case *job.Message:
		return q.handle(t)
	default:
		return errors.New("invalid task type")
	}
}

func (q *Queue) handle(m *job.Message) error {
	// create channel with buffer size 1 to avoid goroutine leak
	done := make(chan error, 1)
	panicChan := make(chan interface{}, 1)
	startTime := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), m.Timeout)
	defer func() {
		cancel()
	}()

	// run the job
	go func() {
		// handle panic issue
		defer func() {
			if p := recover(); p != nil {
				panicChan <- p
			}
		}()

		// run custom process function
		var err error

		b := &backoff.Backoff{
			Min:    m.RetryMin,
			Max:    m.RetryMax,
			Factor: m.RetryFactor,
			Jitter: m.Jitter,
		}
		delay := m.RetryDelay
	loop:
		for {
			if m.Task != nil {
				err = m.Task(ctx)
			} else {
				err = q.worker.Run(ctx, m)
			}

			// check error and retry count
			if err == nil || m.RetryCount == 0 {
				break
			}
			m.RetryCount--

			if m.RetryDelay == 0 {
				delay = b.Duration()
			}

			select {
			case <-time.After(delay): // retry delay
				q.logger.Infof("retry remaining times: %d, delay time: %s", m.RetryCount, delay)
			case <-ctx.Done(): // timeout reached
				err = ctx.Err()
				break loop
			}
		}

		done <- err
	}()

	select {
	case p := <-panicChan:
		panic(p)
	case <-ctx.Done(): // timeout reached
		return ctx.Err()
	case <-q.quit: // shutdown service
		// cancel job
		cancel()

		leftTime := m.Timeout - time.Since(startTime)
		// wait job
		select {
		case <-time.After(leftTime):
			return context.DeadlineExceeded
		case err := <-done: // job finish
			return err
		case p := <-panicChan:
			panic(p)
		}
	case err := <-done: // job finish
		return err
	}
}

// UpdateWorkerCount to update worker number dynamically.
func (q *Queue) UpdateWorkerCount(num int64) {
	q.Lock()
	q.workerCount = num
	q.Unlock()
	q.schedule()
}

// schedule to check worker number
func (q *Queue) schedule() {
	q.Lock()
	defer q.Unlock()
	if q.BusyWorkers() >= q.workerCount {
		return
	}

	select {
	case q.ready <- struct{}{}:
	default:
	}
}

// start to start all worker
func (q *Queue) start() {
	tasks := make(chan core.TaskMessage, 1)

	for {
		// check worker number
		q.schedule()

		select {
		// wait worker ready
		case <-q.ready:
		case <-q.quit:
			return
		}

		// request task from queue in background
		q.routineGroup.Run(func() {
			for {
				t, err := q.worker.Request()
				if t == nil || err != nil {
					if err != nil {
						select {
						case <-q.quit:
							if !errors.Is(err, ErrNoTaskInQueue) {
								close(tasks)
								return
							}
						case <-q.newTaskAdded:
							// New task added
						}
					}
				}
				if t != nil {
					tasks <- t
					return
				}

				select {
				case <-q.quit:
					if !errors.Is(err, ErrNoTaskInQueue) {
						close(tasks)
						return
					}
				default:
				}
			}
		})

		task, ok := <-tasks
		if !ok {
			return
		}

		// start new task
		q.metric.IncBusyWorker()
		q.routineGroup.Run(func() {
			q.work(task)
		})
	}
}

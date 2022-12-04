package queue

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/golang-queue/queue/core"
	"github.com/golang-queue/queue/job"
	"github.com/golang-queue/queue/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

type mockMessage struct {
	message string
}

func (m mockMessage) Bytes() []byte {
	return []byte(m.message)
}

func TestNewQueueWithZeroWorker(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	q, err := NewQueue()
	assert.Error(t, err)
	assert.Nil(t, q)

	w := mocks.NewMockWorker(controller)
	w.EXPECT().Shutdown().Return(nil)
	q, err = NewQueue(
		WithWorker(w),
		WithWorkerCount(0),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	q.Start()
	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, 0, q.BusyWorkers())
	q.Release()
}

func TestNewQueueWithDefaultWorker(t *testing.T) {
	controller := gomock.NewController(t)
	defer controller.Finish()

	q, err := NewQueue()
	assert.Error(t, err)
	assert.Nil(t, q)

	w := mocks.NewMockWorker(controller)
	m := mocks.NewMockQueuedMessage(controller)
	m.EXPECT().Bytes().Return([]byte("test")).AnyTimes()
	w.EXPECT().Shutdown().Return(nil)
	w.EXPECT().Request().Return(m, nil).AnyTimes()
	w.EXPECT().Run(context.Background(), m).Return(nil).AnyTimes()
	q, err = NewQueue(
		WithWorker(w),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	q.Start()
	q.Release()
	assert.Equal(t, 0, q.BusyWorkers())
}

func TestShtdonwOnce(t *testing.T) {
	w := &messageWorker{
		messages: make(chan core.QueuedMessage, 100),
	}
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(2),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	q.Start()
	assert.Equal(t, 0, q.BusyWorkers())
	q.Shutdown()
	// don't panic here
	q.Shutdown()
	q.Wait()
	assert.Equal(t, 0, q.BusyWorkers())
}

func TestCapacityReached(t *testing.T) {
	w := &messageWorker{
		messages: make(chan core.QueuedMessage, 1),
	}
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(5),
		WithLogger(NewEmptyLogger()),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	assert.NoError(t, q.Queue(mockMessage{
		message: "foobar",
	}))
	// max capacity reached
	assert.Error(t, q.Queue(mockMessage{
		message: "foobar",
	}))
}

func TestCloseQueueAfterShutdown(t *testing.T) {
	w := &messageWorker{
		messages: make(chan core.QueuedMessage, 10),
	}
	q, err := NewQueue(
		WithWorker(w),
		WithWorkerCount(5),
		WithLogger(NewEmptyLogger()),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	assert.NoError(t, q.Queue(mockMessage{
		message: "foobar",
	}))
	q.Shutdown()
	err = q.Queue(mockMessage{
		message: "foobar",
	})
	assert.Error(t, err)
	assert.Equal(t, ErrQueueShutdown, err)
	err = q.Queue(mockMessage{
		message: "foobar",
	}, job.WithTimeout(10*time.Millisecond))
	assert.Error(t, err)
	assert.Equal(t, ErrQueueShutdown, err)
}

func TestHandleTimeout(t *testing.T) {
	m := &job.Message{
		Timeout: 100 * time.Millisecond,
		Payload: []byte("foo"),
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m core.QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return nil
		}),
	)

	q, err := NewQueue(
		WithWorker(w),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	err = q.handle(m)
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)

	done := make(chan error)
	go func() {
		done <- q.handle(m)
	}()

	err = <-done
	assert.Error(t, err)
	assert.Equal(t, context.DeadlineExceeded, err)
}

func TestJobComplete(t *testing.T) {
	m := &job.Message{
		Timeout: 100 * time.Millisecond,
		Payload: []byte("foo"),
	}
	w := NewConsumer(
		WithFn(func(ctx context.Context, m core.QueuedMessage) error {
			return errors.New("job completed")
		}),
	)

	q, err := NewQueue(
		WithWorker(w),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	err = q.handle(m)
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)

	m = &job.Message{
		Timeout: 250 * time.Millisecond,
		Payload: []byte("foo"),
	}

	w = NewConsumer(
		WithFn(func(ctx context.Context, m core.QueuedMessage) error {
			time.Sleep(200 * time.Millisecond)
			return errors.New("job completed")
		}),
	)

	q, err = NewQueue(
		WithWorker(w),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	err = q.handle(m)
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)
}

func TestTaskJobComplete(t *testing.T) {
	m := &job.Message{
		Timeout: 100 * time.Millisecond,
		Task: func(ctx context.Context) error {
			return errors.New("job completed")
		},
	}
	w := NewConsumer()

	q, err := NewQueue(
		WithWorker(w),
	)
	assert.NoError(t, err)
	assert.NotNil(t, q)

	err = q.handle(m)
	assert.Error(t, err)
	assert.Equal(t, errors.New("job completed"), err)

	m = &job.Message{
		Timeout: 250 * time.Millisecond,
		Task: func(ctx context.Context) error {
			return nil
		},
	}

	assert.NoError(t, q.handle(m))

	// job timeout
	m = &job.Message{
		Timeout: 50 * time.Millisecond,
		Task: func(ctx context.Context) error {
			time.Sleep(60 * time.Millisecond)
			return nil
		},
	}
	assert.Equal(t, context.DeadlineExceeded, q.handle(m))
}

package nsq

import (
	"testing"
	"time"

	"github.com/appleboy/queue"

	"github.com/stretchr/testify/assert"
)

var host = "nsq"

func TestDefaultFlow(t *testing.T) {
	m := &Job{
		Body: []byte("foo"),
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("test"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(100 * time.Millisecond)
	assert.NoError(t, q.Queue(m))
	m.Body = []byte("new message")
	assert.NoError(t, q.Queue(m))
	q.Shutdown()
	q.Wait()
}

func TestShutdown(t *testing.T) {
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("test"),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
	)
	assert.NoError(t, err)
	q.Start()
	time.Sleep(1 * time.Second)
	q.Shutdown()
	// check shutdown once
	q.Shutdown()
	q.Wait()
}

func TestCustomFuncAndWait(t *testing.T) {
	m := &Job{
		Body: []byte("foo"),
	}
	w := NewWorker(
		WithAddr(host+":4150"),
		WithTopic("test"),
		WithMaxInFlight(2),
		WithRunFunc(func(msg queue.QueuedMessage) error {
			time.Sleep(500 * time.Millisecond)
			return nil
		}),
	)
	q, err := queue.NewQueue(
		queue.WithWorker(w),
		queue.WithWorkerCount(2),
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

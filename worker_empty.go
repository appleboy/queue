package queue

var _ Worker = (*emptyWorker)(nil)

// just for unit testing, don't use it.
type emptyWorker struct{}

func (w *emptyWorker) Run(task QueuedMessage) error    { return nil }
func (w *emptyWorker) Shutdown() error                 { return nil }
func (w *emptyWorker) Queue(task QueuedMessage) error  { return nil }
func (w *emptyWorker) Request() (QueuedMessage, error) { return nil, nil }
func (w *emptyWorker) Capacity() int                   { return 0 }
func (w *emptyWorker) Usage() int                      { return 0 }
func (w *emptyWorker) BusyWorkers() uint64             { return uint64(0) }

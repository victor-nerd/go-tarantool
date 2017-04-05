package queue

type Task struct {
	id     uint64
	status string
	data   interface{}
	q      *queue
}

// Return a task id
func (t *Task) GetId() uint64 {
	return t.id
}

// Return a task data
func (t *Task) GetData() interface{} {
	return t.data
}

// Return a task status
func (t *Task) GetStatus() string {
	return t.status
}

// Signal that the task has been completed
func (t *Task) Ack() error {
	return t.produce(t.q._ack)
}

// Delete task from queue
func (t *Task) Delete() error {
	return t.produce(t.q._delete)
}

// If it becomes clear that a task cannot be executed in the current circumstances, you can "bury" the task -- that is, disable it until the circumstances change.
func (t *Task) Bury() error {
	return t.produce(t.q._bury)
}

func (t *Task) produce(f func(taskId uint64) (string, error)) error {
	newStatus, err := f(t.id)
	if err != nil {
		return err
	}

	t.status = newStatus
	return nil
}

// Put the task back in the queue, 'release' implies unsuccessful completion of a taken task.
func (t *Task) Release() error {
	return t.release(Opts{})
}

// Put the task back in the queue with config, 'release' implies unsuccessful completion of a taken task.
func (t *Task) ReleaseWithConfig(cfg Opts) error {
	return t.release(cfg)
}

func (t *Task) release(cfg Opts) error {
	newStatus, err := t.q._release(t.id, cfg)
	if err != nil {
		return err
	}

	t.status = newStatus
	return nil
}

// Return true if task status is ready
func (t *Task) IsReady() bool {
	return t.status == READY
}

// Return true if task status is taken
func (t *Task) IsTaken() bool {
	return t.status == TAKEN
}

// Return true if task status is done
func (t *Task) IsDone() bool {
	return t.status == DONE
}

// Return true if task status is bury
func (t *Task) IsBuried() bool {
	return t.status == BURIED
}

// Return true if task status is delayed
func (t *Task) IsDelayed() bool {
	return t.status == DELAYED
}

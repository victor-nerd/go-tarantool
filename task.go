package tarantool

type Task struct {
	id     uint64
	status string
	data   interface{}
	q      *queue
}

func (t *Task) GetId() uint64 {
	return t.id
}

func (t *Task) GetData() interface{} {
	return t.data
}

func (t *Task) GetStatus() string {
	return t.status
}

func (t *Task) Ack() error {
	return t.produce(t.q._ack)
}

func (t *Task) Delete() error {
	return t.produce(t.q._delete)
}

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

func (t *Task) Release() error {
	return t.release(QueueOpts{})
}

func (t *Task) ReleaseWithCinfig(cfg QueueOpts) error {
	return t.release(cfg)
}

func (t *Task) release(cfg QueueOpts) error {
	newStatus, err := t.q._release(t.id, cfg)
	if err != nil {
		return err
	}

	t.status = newStatus
	return nil
}

func (t *Task) IsReady() bool {
	return t.status == READY
}

func (t *Task) IsTaken() bool {
	return t.status == TAKEN
}

func (t *Task) IsDone() bool {
	return t.status == DONE
}

func (t *Task) IsBuried() bool {
	return t.status == BURIED
}

func (t *Task) IsDelayed() bool {
	return t.status == DELAYED
}

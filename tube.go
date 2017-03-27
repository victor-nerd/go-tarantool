package tarantool

type TubeData struct {
	id uint64
	status string
	data interface{}
}

var DEFAULT_TUBE_DATA = TubeData{0, "", nil}

func (t TubeData) GetId() uint64 {
	return t.id
}

func (t TubeData) GetData() interface{} {
	return t.data
}

func (t TubeData) GetStatus() string {
	return t.status
}

func (t TubeData) isReady() bool {
	return t.status == READY
}

func (t TubeData) isTaken() bool {
	return t.status == TAKEN
}

func (t TubeData) isDone() bool {
	return t.status == DONE
}

func (t TubeData) isBuried() bool {
	return t.status == BURIED
}

func (t TubeData) isDelayed() bool {
	return t.status == DELAYED
}
package queue

import (
	"fmt"
	"github.com/tarantool/go-tarantool"
	"strings"
	"time"
)

type Queue interface {
	Put(data interface{}) (*Task, error)
	PutWithOpts(data interface{}, cfg Opts) (*Task, error)
	Take() (*Task, error)
	TakeTimeout(timeout time.Duration) (*Task, error)
	Drop() error
	Peek(taskId uint64) (*Task, error)
	Kick(taskId uint64) (uint64, error)
	Statistic() (interface{}, error)
}

type queue struct {
	name string
	conn *tarantool.Connection
	cmds *cmd
}

type cmd struct {
	put        string
	take       string
	drop       string
	peek       string
	ack        string
	delete     string
	bury       string
	kick       string
	release    string
	statistics string
}

type Cfg struct {
	Temporary   bool // if true, the contents do not persist on disk
	IfNotExists bool // if true, no error will be returned if the tube already exists
	Kind        queueType
	Opts
}

func (cfg Cfg) toString() string {
	params := []string{fmt.Sprintf("temporary = %v, if_not_exists = %v", cfg.Temporary, cfg.IfNotExists)}

	if cfg.Ttl.Seconds() != 0 {
		params = append(params, fmt.Sprintf("ttl = %f", cfg.Ttl.Seconds()))
	}

	if cfg.Ttr.Seconds() != 0 {
		params = append(params, fmt.Sprintf("ttr = %f", cfg.Ttr.Seconds()))
	}

	if cfg.Delay.Seconds() != 0 {
		params = append(params, fmt.Sprintf("delay = %f", cfg.Delay.Seconds()))
	}

	return "{" + strings.Join(params, ",") + "}"
}

func (cfg Cfg) getType() string {
	kind := string(cfg.Kind)
	if kind == "" {
		kind = string(FIFO)
	}

	return kind
}

type Opts struct {
	Pri   int           // task priorities
	Ttl   time.Duration // task time to live
	Ttr   time.Duration // task time to execute
	Delay time.Duration // delayed execution
}

func (opts Opts) toMap() map[string]interface{} {
	ret := make(map[string]interface{})

	if opts.Ttl.Seconds() != 0 {
		ret["ttl"] = opts.Ttl.Seconds()
	}

	if opts.Ttr.Seconds() != 0 {
		ret["ttr"] = opts.Ttr.Seconds()
	}

	if opts.Delay.Seconds() != 0 {
		ret["delay"] = opts.Delay.Seconds()
	}

	if opts.Pri != 0 {
		ret["pri"] = opts.Pri
	}

	return ret
}

// New creates a new queue with config and return Queue.
func NewQueue(conn *tarantool.Connection, name string, cfg Cfg) (Queue, error) {
	var q *queue
	cmd := fmt.Sprintf("queue.create_tube('%s', '%s', %s)", name, cfg.getType(), cfg.toString())
	_, err := conn.Eval(cmd, []interface{}{})
	if err == nil {
		q = &queue{
			name,
			conn,
			makeCmd(name),
		}
	}
	return q, err
}

// GetQueue returns an existing queue by name.
func GetQueue(conn *tarantool.Connection, name string) (Queue, error) {
	var q *queue
	cmd := fmt.Sprintf("return queue.tube.%s ~= null", name)
	resp, err := conn.Eval(cmd, []interface{}{})
	if err != nil {
		return q, err
	}

	exist := len(resp.Data) != 0 && resp.Data[0].(bool)

	if exist {
		q = &queue{
			name,
			conn,
			makeCmd(name),
		}
	} else {
		err = fmt.Errorf("Tube %s does not exist", name)
	}

	return q, err
}

// Put data to queue. Returns task.
func (q *queue) Put(data interface{}) (*Task, error) {
	return q.put(data)
}

// Put data with options (ttl/ttr/pri/delay) to queue. Returns task.
func (q *queue) PutWithOpts(data interface{}, cfg Opts) (*Task, error) {
	return q.put(data, cfg.toMap())
}

func (q *queue) put(params ...interface{}) (*Task, error) {
	var task *Task
	resp, err := q.conn.Call(q.cmds.put, params)
	if err == nil {
		task, err = toTask(resp.Data, q)
	}

	return task, err
}

// The take request searches for a task in the queue.
func (q *queue) Take() (*Task, error) {
	var params interface{}
	if q.conn.ConfiguredTimeout() > 0 {
		params = q.conn.ConfiguredTimeout().Seconds()
	}
	return q.take(params)
}

// The take request searches for a task in the queue. Waits until a task becomes ready or the timeout expires.
func (q *queue) TakeTimeout(timeout time.Duration) (*Task, error) {
	t := q.conn.ConfiguredTimeout()
	if t > 0 && timeout > t {
		timeout = t
	}
	return q.take(timeout.Seconds())
}

func (q *queue) take(params interface{}) (*Task, error) {
	var t *Task
	resp, err := q.conn.Call(q.cmds.take, []interface{}{params})
	if err == nil {
		t, err = toTask(resp.Data, q)
	}
	return t, err
}

// Drop queue.
func (q *queue) Drop() error {
	_, err := q.conn.Call(q.cmds.drop, []interface{}{})
	return err
}

// Look at a task without changing its state.
func (q *queue) Peek(taskId uint64) (*Task, error) {
	resp, err := q.conn.Call(q.cmds.peek, []interface{}{taskId})
	if err != nil {
		return nil, err
	}

	t, err := toTask(resp.Data, q)

	return t, err
}

func (q *queue) _ack(taskId uint64) (string, error) {
	return q.produce(q.cmds.ack, taskId)
}

func (q *queue) _delete(taskId uint64) (string, error) {
	return q.produce(q.cmds.delete, taskId)
}

func (q *queue) _bury(taskId uint64) (string, error) {
	return q.produce(q.cmds.bury, taskId)
}

func (q *queue) _release(taskId uint64, cfg Opts) (string, error) {
	return q.produce(q.cmds.release, taskId, cfg.toMap())
}
func (q *queue) produce(cmd string, params ...interface{}) (string, error) {
	resp, err := q.conn.Call(cmd, params)
	if err != nil {
		return "", err
	}

	t, err := toTask(resp.Data, q)
	if err != nil {
		return "", err
	}
	return t.status, nil
}

// Reverse the effect of a bury request on one or more tasks.
func (q *queue) Kick(count uint64) (uint64, error) {
	resp, err := q.conn.Call(q.cmds.kick, []interface{}{count})
	var id uint64
	if err == nil {
		id = resp.Data[0].([]interface{})[0].(uint64)
	}
	return id, err
}

// Return the number of tasks in a queue broken down by task_state, and the number of requests broken down by the type of request.
func (q *queue) Statistic() (interface{}, error) {
	resp, err := q.conn.Call(q.cmds.statistics, []interface{}{q.name})
	if err != nil {
		return nil, err
	}

	if len(resp.Data) != 0 {
		data, ok := resp.Data[0].([]interface{})
		if ok && len(data) != 0 {
			return data[0], nil
		}
	}

	return nil, nil
}

func makeCmd(name string) *cmd {
	return &cmd{
		put:        "queue.tube." + name + ":put",
		take:       "queue.tube." + name + ":take",
		drop:       "queue.tube." + name + ":drop",
		peek:       "queue.tube." + name + ":peek",
		ack:        "queue.tube." + name + ":ack",
		delete:     "queue.tube." + name + ":delete",
		bury:       "queue.tube." + name + ":bury",
		kick:       "queue.tube." + name + ":kick",
		release:    "queue.tube." + name + ":release",
		statistics: "queue.statistics",
	}
}

func toTask(responseData []interface{}, q *queue) (*Task, error) {
	if len(responseData) != 0 {
		data, ok := responseData[0].([]interface{})
		if ok && len(data) >= 3 {
			return &Task{
				data[0].(uint64),
				data[1].(string),
				data[2],
				q,
			}, nil
		}
	}

	return nil, nil
}

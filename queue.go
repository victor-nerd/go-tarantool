package tarantool

import (
	"fmt"
	"strings"
	"time"
)

type Queue interface {
	Put(data interface{}) (*Task, error)
	PutWithConfig(data interface{}, cfg QueueOpts) (*Task, error)
	Take() (*Task, error)
	TakeWithTimeout(timeout time.Duration) (*Task, error)
	Drop() error
	Peek(taskId uint64) (*Task, error)
	Kick(taskId uint64) (uint64, error)
	Statistic() (interface{}, error)
}

type queue struct {
	name string
	conn *Connection
	cmd  map[string]string
}

type queueCfg interface {
	String() string
	Type() string
}

type QueueCfg struct {
	Temporary   bool
	IfNotExists bool
	Kind        queueType
}

func (cfg QueueCfg) String() string {
	return fmt.Sprintf("{ temporary = %v, if_not_exists = %v }", cfg.Temporary, cfg.IfNotExists)
}

func (cfg QueueCfg) Type() string {
	return string(cfg.Kind)
}

type TtlQueueCfg struct {
	QueueCfg
	QueueOpts
}

type QueueOpts struct {
	Pri   int
	Ttl   time.Duration
	Ttr   time.Duration
	Delay time.Duration
}

func (cfg TtlQueueCfg) String() string {
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

func (cfg TtlQueueCfg) Type() string {
	kind := string(cfg.Kind)
	if kind == "" {
		kind = string(FIFO_QUEUE)
	}

	return kind
}

func (opts QueueOpts) toMap() map[string]interface{} {
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

func newQueue(conn *Connection, name string, cfg queueCfg) (Queue, error) {
	var q queue
	cmd := fmt.Sprintf("queue.create_tube('%s', '%s', %s)", name, cfg.Type(), cfg.String())
	_, err := conn.Eval(cmd, []interface{}{})
	if err == nil {
		q = queue{
			name,
			conn,
			makeCmdMap(name),
		}
	}
	return q, err
}

func getQueue(conn *Connection, name string) (Queue, error) {
	var q queue
	cmd := fmt.Sprintf("return queue.tube.%s ~= null", name)
	resp, err := conn.Eval(cmd, []interface{}{})
	if err != nil {
		return q, err
	}

	exist := len(resp.Data) != 0 && resp.Data[0].(bool)

	if exist {
		q = queue{
			name,
			conn,
			makeCmdMap(name),
		}
	} else {
		err = fmt.Errorf("Tube %s does not exist", name)
	}

	return q, err
}

func (q queue) Put(data interface{}) (*Task, error) {
	return q.put(data)
}

func (q queue) PutWithConfig(data interface{}, cfg QueueOpts) (*Task, error) {
	return q.put(data, cfg.toMap())
}

func (q queue) put(p ...interface{}) (*Task, error) {
	var (
		params []interface{}
		task   *Task
	)
	params = append(params, p...)
	resp, err := q.conn.Call(q.cmd["put"], params)
	if err == nil {
		task, err = toTask(resp.Data, &q)
	}

	return task, err
}

func (q queue) Take() (*Task, error) {
	var params interface{}
	if q.conn.opts.Timeout > 0 {
		params = q.conn.opts.Timeout.Seconds()
	}
	return q.take(params)
}

func (q queue) TakeWithTimeout(timeout time.Duration) (*Task, error) {
	if q.conn.opts.Timeout > 0 && timeout > q.conn.opts.Timeout {
		timeout = q.conn.opts.Timeout
	}
	return q.take(timeout.Seconds())
}

func (q queue) take(params interface{}) (*Task, error) {
	var t *Task
	resp, err := q.conn.Call(q.cmd["take"], []interface{}{params})
	if err == nil {
		t, err = toTask(resp.Data, &q)
	}
	return t, err
}

func (q queue) Drop() error {
	_, err := q.conn.Call(q.cmd["drop"], []interface{}{})
	return err
}

func (q queue) Peek(taskId uint64) (*Task, error) {
	resp, err := q.conn.Call(q.cmd["peek"], []interface{}{taskId})
	if err != nil {
		return nil, err
	}

	t, err := toTask(resp.Data, &q)

	return t, err
}

func (q queue) _ack(taskId uint64) (string, error) {
	return q.produce("ack", taskId)
}

func (q queue) _delete(taskId uint64) (string, error) {
	return q.produce("delete", taskId)
}

func (q queue) _bury(taskId uint64) (string, error) {
	return q.produce("bury", taskId)
}

func (q queue) _release(taskId uint64, cfg QueueOpts) (string, error) {
	return q.produce("release", taskId, cfg.toMap())
}
func (q queue) produce(cmd string, p ...interface{}) (string, error) {
	var params []interface{}
	params = append(params, p...)
	resp, err := q.conn.Call(q.cmd[cmd], params)
	if err != nil {
		return "", err
	}

	t, err := toTask(resp.Data, &q)
	if err != nil {
		return "", err
	}
	return t.status, nil
}

func (q queue) Kick(count uint64) (uint64, error) {
	resp, err := q.conn.Call(q.cmd["kick"], []interface{}{count})
	var id uint64
	if err == nil {
		id = resp.Data[0].([]interface{})[0].(uint64)
	}
	return id, err
}

func (q queue) Statistic() (interface{}, error) {
	resp, err := q.conn.Call(q.cmd["statistics"], []interface{}{})
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

func makeCmdMap(name string) map[string]string {
	return map[string]string{
		"put":        "queue.tube." + name + ":put",
		"take":       "queue.tube." + name + ":take",
		"drop":       "queue.tube." + name + ":drop",
		"peek":       "queue.tube." + name + ":peek",
		"ack":        "queue.tube." + name + ":ack",
		"delete":     "queue.tube." + name + ":delete",
		"bury":       "queue.tube." + name + ":bury",
		"kick":       "queue.tube." + name + ":kick",
		"release":    "queue.tube." + name + ":release",
		"statistics": "queue.statistics",
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

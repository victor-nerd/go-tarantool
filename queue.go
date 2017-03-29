package tarantool

import (
	"fmt"
	"strings"
	"time"
)

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

	ret["pri"] = opts.Pri

	return ret
}

func newQueue(conn *Connection, name string, cfg queueCfg) (queue, error) {
	var q queue
	cmd := fmt.Sprintf("queue.create_tube('%s', '%s', %s)", name, cfg.Type(), cfg.String())
	fut := conn.EvalAsync(cmd, []interface{}{})
	fut.wait()
	if fut.err == nil {
		q = queue{
			name,
			conn,
			makeCmdMap(name),
		}
	}
	return q, fut.err
}

func getQueue(conn *Connection, name string) (queue, error) {
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

func (q queue) Put(data interface{}) (uint64, error) {
	return q.put(data)
}

func (q queue) PutWithConfig(data interface{}, cfg QueueOpts) (uint64, error) {
	return q.put(data, cfg.toMap())
}

func (q queue) put(p ...interface{}) (uint64, error) {
	var (
		params []interface{}
		id     uint64
	)
	params = append(params, p...)
	resp, err := q.conn.Call(q.cmd["put"], params)
	if err == nil {
		var task *Task
		task, err = toTask(resp.Data, &q)
		if err == nil {
			id = task.id
		}
	}

	return id, err
}

func (q queue) Take() (*Task, error) {
	return q.take(nil)
}

func (q queue) TakeWithTimeout(timeout time.Duration) (*Task, error) {
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

func (q queue) _ack(taskId uint64) error {
	_, err := q.conn.Call(q.cmd["ack"], []interface{}{taskId})
	return err
}

func (q queue) _delete(taskId uint64) error {
	_, err := q.conn.Call(q.cmd["delete"], []interface{}{taskId})
	return err
}

func (q queue) _bury(taskId uint64) error {
	_, err := q.conn.Call(q.cmd["bury"], []interface{}{taskId})
	return err
}

func (q queue) Kick(taskId uint64) (uint64, error) {
	resp, err := q.conn.Call(q.cmd["kick"], []interface{}{taskId})
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

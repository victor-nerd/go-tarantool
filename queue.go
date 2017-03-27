package tarantool

import (
	"fmt"
	"time"
	"strings"
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
	Temporary bool
	IfNotExists bool
	Kind queueType
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
	Pri int
	Ttl time.Duration
	Ttr time.Duration
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

	return ret
}

func newQueue(conn *Connection, name string, cfg queueCfg) (queue, error) {
	cmd := fmt.Sprintf("queue.create_tube('%s', '%s', %s)", name, cfg.Type(), cfg.String())
	fmt.Println("STAETL: ", cmd)
	fut := conn.EvalAsync(cmd, []interface{}{})
	fut.wait()
	return queue{
		name,
		conn,
		makeCmdMap(name),
	}, fut.err
}

func (q queue) Put(data interface{}) (uint64, error) {
	resp, err := q.conn.Call(q.cmd["put"], []interface{}{data})
	return convertRsponseToTubeData(resp.Data).id, err
}

func (q queue) PutWithConfig(data interface{}, cfg QueueOpts) (uint64, error) {
	resp, err := q.conn.Call(q.cmd["put"], []interface{}{data, cfg.toMap()})
	return convertRsponseToTubeData(resp.Data).id, err
}

func (q queue) Take() (TubeData, error) {
	return q.take(nil)
}

func (q queue) TakeWithTimeout(timeout time.Duration) (TubeData, error) {
	return q.take(timeout.Seconds())
}

func (q queue) take(params interface{}) (TubeData, error) {
	var t TubeData
	resp, err := q.conn.Call(q.cmd["take"], []interface{}{params})
	if err == nil && len(resp.Data) != 0 {
		data, ok := resp.Data[0].([]interface{})
		if ok && len(data) >= 3 {
			t = TubeData{
				data[0].(uint64),
				data[1].(string),
				data[2],
			}
		}
	}
	return t, err
}

func (q queue) Drop() error {
	_, err := q.conn.Call(q.cmd["drop"], []interface{}{})
	return err
}

func (q queue) Peek(taskId uint64) (TubeData, error) {
	resp, err := q.conn.Call(q.cmd["peek"], []interface{}{taskId})
	if err != nil {
		return DEFAULT_TUBE_DATA, err
	}

	return convertRsponseToTubeData(resp.Data), nil
}

func (q queue) Ack(taskId uint64) error {
	_, err := q.conn.Call(q.cmd["ack"], []interface{}{taskId})
	return err
}

func (q queue) Delete(taskId uint64) error {
	_, err := q.conn.Call(q.cmd["delete"], []interface{}{taskId})
	return err
}

func (q queue) Bury(taskId uint64) error {
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

func convertRsponseToTubeData(responseData []interface{}) TubeData {
	t := DEFAULT_TUBE_DATA
	if len(responseData) != 0 {
		data, ok := responseData[0].([]interface{})
		if ok && len(data) >= 3 {
			t.id = data[0].(uint64)
			t.status = data[1].(string)
			t.data = data[2]
		}
	}

	return t
}

package tarantool

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

type Request struct {
	conn        *Connection
	requestId   uint32
	requestCode int32
}

type Future struct {
	req     *Request
	resp    Response
	err     error
	ready   chan struct{}
	timeout *time.Timer
}

func (conn *Connection) NewRequest(requestCode int32) (req *Request) {
	req = &Request{}
	req.conn = conn
	req.requestId = conn.nextRequestId()
	req.requestCode = requestCode
	return
}

func (conn *Connection) Ping() (resp *Response, err error) {
	request := conn.NewRequest(PingRequest)
	return request.future(func(enc *msgpack.Encoder)error{enc.EncodeMapLen(0);return nil}).Get()
}

func (req *Request) fillSearch(enc *msgpack.Encoder, spaceNo, indexNo uint32, key []interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyIndexNo)
	enc.EncodeUint64(uint64(indexNo))
	enc.EncodeUint64(KeyKey)
	return enc.Encode(key)
}

func (req *Request) fillIterator(enc *msgpack.Encoder, offset, limit, iterator uint32) {
	enc.EncodeUint64(KeyIterator)
	enc.EncodeUint64(uint64(iterator))
	enc.EncodeUint64(KeyOffset)
	enc.EncodeUint64(uint64(offset))
	enc.EncodeUint64(KeyLimit)
	enc.EncodeUint64(uint64(limit))
}

func (req *Request) fillInsert(enc *msgpack.Encoder, spaceNo uint32, tuple interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyTuple)
	return enc.Encode(tuple)
}

func (conn *Connection) Select(space, index interface{}, offset, limit, iterator uint32, key []interface{}) (resp *Response, err error) {
	return conn.SelectAsync(space, index, offset, limit, iterator, key).Get()
}

func (conn *Connection) Insert(space interface{}, tuple interface{}) (resp *Response, err error) {
	return conn.InsertAsync(space, tuple).Get()
}

func (conn *Connection) Replace(space interface{}, tuple interface{}) (resp *Response, err error) {
	return conn.ReplaceAsync(space, tuple).Get()
}

func (conn *Connection) Delete(space, index interface{}, key []interface{}) (resp *Response, err error) {
	return conn.DeleteAsync(space, index, key).Get()
}

func (conn *Connection) Update(space, index interface{}, key, ops []interface{}) (resp *Response, err error) {
	return conn.UpdateAsync(space, index, key, ops).Get()
}

func (conn *Connection) Upsert(space interface{}, tuple, ops []interface{}) (resp *Response, err error) {
	return conn.UpsertAsync(space, tuple, ops).Get()
}

func (conn *Connection) Call(functionName string, args []interface{}) (resp *Response, err error) {
	return conn.CallAsync(functionName, args).Get()
}

func (conn *Connection) Eval(expr string, args []interface{}) (resp *Response, err error) {
	return conn.EvalAsync(expr, args).Get()
}

// Typed methods
func (conn *Connection) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key []interface{}, result interface{}) (err error) {
	return conn.SelectAsync(space, index, offset, limit, iterator, key).GetTyped(result)
}

func (conn *Connection) InsertTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return conn.InsertAsync(space, tuple).GetTyped(result)
}

func (conn *Connection) ReplaceTyped(space interface{}, tuple interface{}, result interface{}) (err error) {
	return conn.ReplaceAsync(space, tuple).GetTyped(result)
}

func (conn *Connection) DeleteTyped(space, index interface{}, key []interface{}, result interface{}) (err error) {
	return conn.DeleteAsync(space, index, key).GetTyped(result)
}

func (conn *Connection) UpdateTyped(space, index interface{}, key, ops []interface{}, result interface{}) (err error) {
	return conn.UpdateAsync(space, index, key, ops).GetTyped(result)
}

func (conn *Connection) UpsertTyped(space interface{}, tuple, ops []interface{}, result interface{}) (err error) {
	return conn.UpsertAsync(space, tuple, ops).GetTyped(result)
}

func (conn *Connection) CallTyped(functionName string, args []interface{}, result interface{}) (err error) {
	return conn.CallAsync(functionName, args).GetTyped(result)
}

func (conn *Connection) EvalTyped(expr string, args []interface{}, result interface{}) (err error) {
	return conn.EvalAsync(expr, args).GetTyped(result)
}

// Async methods
func (conn *Connection) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key []interface{}) *Future {
	request := conn.NewRequest(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(6)
		request.fillIterator(enc, offset, limit, iterator)
		return request.fillSearch(enc, spaceNo, indexNo, key)
	})
}

func (conn *Connection) InsertAsync(space interface{}, tuple interface{}) *Future {
	request := conn.NewRequest(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return request.fillInsert(enc, spaceNo, tuple)
	})
}

func (conn *Connection) ReplaceAsync(space interface{}, tuple interface{}) *Future {
	request := conn.NewRequest(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return request.fillInsert(enc, spaceNo, tuple)
	})
}

func (conn *Connection) DeleteAsync(space, index interface{}, key []interface{}) *Future {
	request := conn.NewRequest(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		return request.fillSearch(enc, spaceNo, indexNo, key)
	})
}

func (conn *Connection) UpdateAsync(space, index interface{}, key, ops []interface{}) *Future {
	request := conn.NewRequest(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(4)
		if err := request.fillSearch(enc, spaceNo, indexNo, key); err != nil {
			return err
		}
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(ops)
	})
}

func (conn *Connection) UpsertAsync(space interface{}, tuple interface{}, ops []interface{}) *Future {
	request := conn.NewRequest(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		enc.EncodeUint64(KeySpaceNo)
		enc.EncodeUint64(uint64(spaceNo))
		enc.EncodeUint64(KeyTuple)
		if err := enc.Encode(tuple); err != nil {
			return err
		}
		enc.EncodeUint64(KeyDefTuple)
		return enc.Encode(ops)
	})
}

func (conn *Connection) CallAsync(functionName string, args []interface{}) *Future {
	request := conn.NewRequest(CallRequest)
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyFunctionName)
		enc.EncodeString(functionName)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

func (conn *Connection) EvalAsync(expr string, args []interface{}) *Future {
	request := conn.NewRequest(EvalRequest)
	return request.future(func (enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyExpression)
		enc.EncodeString(expr)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

//
// private
//

func (req *Request) pack(body func (*msgpack.Encoder)error) (packet []byte, err error) {
	rid := req.requestId
	h := make(smallWBuf, 0, 48)
	h = append(h, smallWBuf{
		0xce, 0, 0, 0, 0, // length
		0x82,                           // 2 element map
		KeyCode, byte(req.requestCode), // request code
		KeySync, 0xce,
		byte(rid >> 24), byte(rid >> 16),
		byte(rid >> 8), byte(rid),
	}...)

	enc := msgpack.NewEncoder(&h)
	if err = body(enc); err != nil {
		return
	}

	l := uint32(len(h) - 5)
	h[1] = byte(l >> 24)
	h[2] = byte(l >> 16)
	h[3] = byte(l >> 8)
	h[4] = byte(l)

	packet = h
	return
}

func (req *Request) future(body func (*msgpack.Encoder)error) (fut *Future) {
	fut = &Future{req: req}

	// check connection ready to process packets
	if closed := req.conn.closed; closed {
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		return
	}
	if c := req.conn.c; c == nil {
		fut.err = ClientError{ErrConnectionNotReady, "client connection is not ready"}
		return
	}

	var packet []byte
	if packet, fut.err = req.pack(body); fut.err != nil {
		return
	}

	fut.ready = make(chan struct{})
	req.conn.reqmut.Lock()
	if req.conn.closed {
		req.conn.reqmut.Unlock()
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		return
	}
	req.conn.requests[req.requestId] = fut
	req.conn.reqmut.Unlock()

	var sent bool
	select {
	case req.conn.packets <- (packet):
		sent = true
	default:
	}

	if req.conn.opts.Timeout > 0 {
		fut.timeout = time.AfterFunc(req.conn.opts.Timeout, fut.timeouted)
	}

	if !sent {
		// if connection is totally closed, then req.conn.packets will be full
		// if connection is busy, we can reach timeout
		select {
		case req.conn.packets <- (packet):
		case <-fut.ready:
		}
	}

	return
}

func (fut *Future) timeouted() {
	conn := fut.req.conn
	requestId := fut.req.requestId
	conn.reqmut.Lock()
	if _, ok := conn.requests[requestId]; ok {
		delete(conn.requests, requestId)
		close(fut.ready)
		fut.err = fmt.Errorf("client timeout for request %d", requestId)
	}
	conn.reqmut.Unlock()
}

func badfuture(err error) *Future {
	return &Future{err: err}
}

func (fut *Future) wait() {
	if fut.ready == nil {
		return
	}
	<-fut.ready
	if fut.timeout != nil {
		fut.timeout.Stop()
		fut.timeout = nil
	}
}

func (fut *Future) Get() (*Response, error) {
	fut.wait()
	if fut.err != nil {
		return &fut.resp, fut.err
	}
	fut.err = fut.resp.decodeBody()
	return &fut.resp, fut.err
}

func (fut *Future) GetTyped(result interface{}) error {
	fut.wait()
	if fut.err != nil {
		return fut.err
	}
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}

var closedtimechan = make(chan time.Time)

func init() {
	close(closedtimechan)
}

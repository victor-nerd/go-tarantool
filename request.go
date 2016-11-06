package tarantool

import (
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"time"
)

type Future struct {
	conn        *Connection
	requestId   uint32
	requestCode int32
	resp        Response
	err         error
	ready       chan struct{}
	timeout     time.Duration
	next        *Future
	time        struct {
		next *Future
		prev **Future
	}
}

func (conn *Connection) newFuture(requestCode int32) (fut *Future) {
	fut = &Future{}
	fut.conn = conn
	fut.requestId = conn.nextRequestId()
	fut.requestCode = requestCode
	return
}

func (conn *Connection) Ping() (resp *Response, err error) {
	future := conn.newFuture(PingRequest)
	return future.send(func(enc *msgpack.Encoder) error { enc.EncodeMapLen(0); return nil }).Get()
}

func (req *Future) fillSearch(enc *msgpack.Encoder, spaceNo, indexNo uint32, key []interface{}) error {
	enc.EncodeUint64(KeySpaceNo)
	enc.EncodeUint64(uint64(spaceNo))
	enc.EncodeUint64(KeyIndexNo)
	enc.EncodeUint64(uint64(indexNo))
	enc.EncodeUint64(KeyKey)
	return enc.Encode(key)
}

func (req *Future) fillIterator(enc *msgpack.Encoder, offset, limit, iterator uint32) {
	enc.EncodeUint64(KeyIterator)
	enc.EncodeUint64(uint64(iterator))
	enc.EncodeUint64(KeyOffset)
	enc.EncodeUint64(uint64(offset))
	enc.EncodeUint64(KeyLimit)
	enc.EncodeUint64(uint64(limit))
}

func (req *Future) fillInsert(enc *msgpack.Encoder, spaceNo uint32, tuple interface{}) error {
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
	future := conn.newFuture(SelectRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	return future.send(func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(6)
		future.fillIterator(enc, offset, limit, iterator)
		return future.fillSearch(enc, spaceNo, indexNo, key)
	})
}

func (conn *Connection) InsertAsync(space interface{}, tuple interface{}) *Future {
	future := conn.newFuture(InsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	return future.send(func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return future.fillInsert(enc, spaceNo, tuple)
	})
}

func (conn *Connection) ReplaceAsync(space interface{}, tuple interface{}) *Future {
	future := conn.newFuture(ReplaceRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	return future.send(func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		return future.fillInsert(enc, spaceNo, tuple)
	})
}

func (conn *Connection) DeleteAsync(space, index interface{}, key []interface{}) *Future {
	future := conn.newFuture(DeleteRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	return future.send(func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(3)
		return future.fillSearch(enc, spaceNo, indexNo, key)
	})
}

func (conn *Connection) UpdateAsync(space, index interface{}, key, ops []interface{}) *Future {
	future := conn.newFuture(UpdateRequest)
	spaceNo, indexNo, err := conn.Schema.resolveSpaceIndex(space, index)
	if err != nil {
		return badfuture(err)
	}
	return future.send(func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(4)
		if err := future.fillSearch(enc, spaceNo, indexNo, key); err != nil {
			return err
		}
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(ops)
	})
}

func (conn *Connection) UpsertAsync(space interface{}, tuple interface{}, ops []interface{}) *Future {
	future := conn.newFuture(UpsertRequest)
	spaceNo, _, err := conn.Schema.resolveSpaceIndex(space, nil)
	if err != nil {
		return badfuture(err)
	}
	return future.send(func(enc *msgpack.Encoder) error {
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
	future := conn.newFuture(CallRequest)
	return future.send(func(enc *msgpack.Encoder) error {
		enc.EncodeMapLen(2)
		enc.EncodeUint64(KeyFunctionName)
		enc.EncodeString(functionName)
		enc.EncodeUint64(KeyTuple)
		return enc.Encode(args)
	})
}

func (conn *Connection) EvalAsync(expr string, args []interface{}) *Future {
	future := conn.newFuture(EvalRequest)
	return future.send(func(enc *msgpack.Encoder) error {
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

func (fut *Future) pack(body func(*msgpack.Encoder) error) (packet []byte, err error) {
	rid := fut.requestId
	h := make(smallWBuf, 0, 48)
	h = append(h, smallWBuf{
		0xce, 0, 0, 0, 0, // length
		0x82,                           // 2 element map
		KeyCode, byte(fut.requestCode), // request code
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

func (fut *Future) send(body func(*msgpack.Encoder) error) *Future {

	// check connection ready to process packets
	if closed := fut.conn.closed; closed {
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		return fut
	}
	if c := fut.conn.c; c == nil {
		fut.err = ClientError{ErrConnectionNotReady, "client connection is not ready"}
		return fut
	}

	var packet []byte
	if packet, fut.err = fut.pack(body); fut.err != nil {
		return fut
	}

	fut.ready = make(chan struct{})
	fut.conn.putFuture(fut)
	if fut.err != nil {
		fut.conn.fetchFuture(fut.requestId)
		return fut
	}

	select {
	case fut.conn.packets <- (packet):
	default:
		// if connection is totally closed, then fut.conn.packets will be full
		// if connection is busy, we can reach timeout
		select {
		case fut.conn.packets <- (packet):
		case <-fut.ready:
		}
	}

	return fut
}

func (fut *Future) timeouted() {
	conn := fut.conn
	if f := conn.fetchFuture(fut.requestId); f != nil {
		if f != fut {
			panic("future doesn't match")
		}
		fut.err = fmt.Errorf("client timeout for request %d", fut.requestId)
		close(fut.ready)
	}
}

func badfuture(err error) *Future {
	return &Future{err: err}
}

func (fut *Future) wait() {
	if fut.ready == nil {
		return
	}
	<-fut.ready
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

package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const requestsMap = 128

var epoch = time.Now()

// Connection is a handle to Tarantool.
//
// It is created and configured with Connect function, and could not be
// reconfigured later.
//
// It is could be "Connected", "Disconnected", and "Closed".
//
// When "Connected" it sends queries to Tarantool.
//
// When "Disconnected" it rejects queries with ClientError{Code: ErrConnectionNotReady}
//
// When "Closed" it rejects queries with ClientError{Code: ErrConnectionClosed}
//
// Connection could become "Closed" when Connection.Close() method called,
// or when Tarantool disconnected and Reconnect pause is not specified or
// MaxReconnects is specified and MaxReconnect reconnect attempts already performed.
//
// You may perform data manipulation operation by calling its methods:
// Call*, Insert*, Replace*, Update*, Upsert*, Call*, Eval*.
//
// In any method that accepts `space` you my pass either space number or
// space name (in this case it will be looked up in schema). Same is true for `index`.
//
// ATTENTION: `tuple`, `key`, `ops` and `args` arguments for any method should be
// and array or should serialize to msgpack array.
//
// ATTENTION: `result` argument for *Typed methods should deserialize from
// msgpack array, cause Tarantool always returns result as an array.
// For all space related methods and Call* (but not Call17*) methods Tarantool
// always returns array of array (array of tuples for space related methods).
// For Eval* and Call17* tarantool always returns array, but does not forces
// array of arrays.
type Connection struct {
	addr  string
	c     *net.TCPConn
	r     *bufio.Reader
	w     *bufio.Writer
	mutex sync.Mutex
	// Schema contains schema loaded on connection.
	Schema    *Schema
	requestId uint32
	// Greeting contains first message sent by tarantool
	Greeting *Greeting

	shard      []connShard
	dirtyShard chan uint32

	control chan struct{}
	rlimit  chan struct{}
	opts    Opts
	closed  bool
	dec     *msgpack.Decoder
	lenbuf  [PacketLengthBytes]byte
}

type connShard struct {
	rmut     sync.Mutex
	requests [requestsMap]struct {
		first *Future
		last  **Future
	}
	bufmut sync.Mutex
	buf    smallWBuf
	enc    *msgpack.Encoder
	_pad   [16]uint64
}

// Greeting is a message sent by tarantool on connect.
type Greeting struct {
	Version string
	auth    string
}

// Opts is a way to configure Connection
type Opts struct {
	// Timeout is requests timeout.
	Timeout time.Duration
	// Reconnect is a pause between reconnection attempts.
	// If specified, then when tarantool is not reachable or disconnected,
	// new connect attempt is performed after pause.
	// By default, no reconnection attempts are performed,
	// so once disconnected, connection becomes Closed.
	Reconnect time.Duration
	// MaxReconnects is a maximum reconnect attempts.
	// After MaxReconnects attempts Connection becomes closed.
	MaxReconnects uint
	// User name for authorization
	User string
	// Pass is password for authorization
	Pass string
	// RateLimit limits number of 'in-fly' request, ie aready putted into
	// requests queue, but not yet answered by server or timeouted.
	// It is disabled by default.
	// See RLimitAction for possible actions when RateLimit.reached.
	RateLimit uint
	// RLimitAction tells what to do when RateLimit reached:
	//   RLimitDrop - immediatly abort request,
	//   RLimitWait - wait during timeout period for some request to be answered.
	//                If no request answered during timeout period, this request
	//                is aborted.
	//                If no timeout period is set, it will wait forever.
	// It is required if RateLimit is specified.
	RLimitAction uint
	// Concurrency is amount of separate mutexes for request
	// queues and buffers inside of connection.
	// It is rounded upto nearest power of 2.
	// By default it is runtime.GOMAXPROCS(-1) * 4
	Concurrency uint32
}

// Connect creates and configures new Connection
//
// Note:
//
// - If opts.Reconnect is zero (default), then connection either already connected
// or error is returned.
//
// - If opts.Reconnect is non-zero, then error will be returned only if authorization// fails. But if Tarantool is not reachable, then it will attempt to reconnect later
// and will not end attempts on authorization failures.
func Connect(addr string, opts Opts) (conn *Connection, err error) {

	conn = &Connection{
		addr:      addr,
		requestId: 0,
		Greeting:  &Greeting{},
		control:   make(chan struct{}),
		opts:      opts,
		dec:       msgpack.NewDecoder(&smallBuf{}),
	}
	maxprocs := uint32(runtime.GOMAXPROCS(-1))
	if conn.opts.Concurrency == 0 || conn.opts.Concurrency > maxprocs*128 {
		conn.opts.Concurrency = maxprocs * 4
	}
	if c := conn.opts.Concurrency; c&(c-1) != 0 {
		for i := uint(1); i < 32; i *= 2 {
			c |= c >> i
		}
		conn.opts.Concurrency = c + 1
	}
	conn.dirtyShard = make(chan uint32, conn.opts.Concurrency)
	conn.shard = make([]connShard, conn.opts.Concurrency)
	for i := range conn.shard {
		shard := &conn.shard[i]
		for j := range shard.requests {
			shard.requests[j].last = &shard.requests[j].first
		}
	}

	if opts.RateLimit > 0 {
		conn.rlimit = make(chan struct{}, opts.RateLimit)
		if opts.RLimitAction != RLimitDrop && opts.RLimitAction != RLimitWait {
			return nil, errors.New("RLimitAction should be specified to RLimitDone nor RLimitWait")
		}
	}

	var reconnect time.Duration
	// disable reconnecting for first connect
	reconnect, conn.opts.Reconnect = conn.opts.Reconnect, 0
	_, _, err = conn.createConnection()
	conn.opts.Reconnect = reconnect
	if err != nil && reconnect == 0 {
		return nil, err
	}

	go conn.writer()
	go conn.reader()
	if conn.opts.Timeout > 0 {
		go conn.timeouts()
	}

	// TODO: reload schema after reconnect
	if err = conn.loadSchema(); err != nil {
		conn.closeConnection(err, nil, nil)
		return nil, err
	}

	return conn, err
}

// Close closes Connection.
// After this method called, there is no way to reopen this Connection.
func (conn *Connection) Close() error {
	err := ClientError{ErrConnectionClosed, "connection closed by client"}
	return conn.closeConnectionForever(err)
}

// RemoteAddr is address of Tarantool socket
func (conn *Connection) RemoteAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.RemoteAddr().String()
}

// LocalAddr is address of outgoing socket
func (conn *Connection) LocalAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.LocalAddr().String()
}

func (conn *Connection) dial() (err error) {
	connection, err := net.Dial("tcp", conn.addr)
	if err != nil {
		return
	}
	c := connection.(*net.TCPConn)
	c.SetNoDelay(true)
	r := bufio.NewReaderSize(c, 128*1024)
	w := bufio.NewWriterSize(c, 128*1024)
	greeting := make([]byte, 128)
	_, err = io.ReadFull(r, greeting)
	if err != nil {
		c.Close()
		return
	}
	conn.Greeting.Version = bytes.NewBuffer(greeting[:64]).String()
	conn.Greeting.auth = bytes.NewBuffer(greeting[64:108]).String()

	// Auth
	if conn.opts.User != "" {
		scr, err := scramble(conn.Greeting.auth, conn.opts.Pass)
		if err != nil {
			err = errors.New("auth: scrambling failure " + err.Error())
			c.Close()
			return err
		}
		if err = conn.writeAuthRequest(w, scr); err != nil {
			c.Close()
			return err
		}
		if err = conn.readAuthResponse(r); err != nil {
			c.Close()
			return err
		}
	}

	// Only if connected and authenticated
	conn.lockShards()
	conn.c = c
	conn.unlockShards()
	conn.r = r
	conn.w = w

	return
}

func (conn *Connection) writeAuthRequest(w *bufio.Writer, scramble []byte) (err error) {
	request := &Future{
		conn:        conn,
		requestId:   0,
		requestCode: AuthRequest,
	}
	var packet smallWBuf
	err = request.pack(&packet, msgpack.NewEncoder(&packet), func(enc *msgpack.Encoder) error {
		return enc.Encode(map[uint32]interface{}{
			KeyUserName: conn.opts.User,
			KeyTuple:    []interface{}{string("chap-sha1"), string(scramble)},
		})
	})
	if err != nil {
		return errors.New("auth: pack error " + err.Error())
	}
	if err := write(w, packet); err != nil {
		return errors.New("auth: write error " + err.Error())
	}
	if err = w.Flush(); err != nil {
		return errors.New("auth: flush error " + err.Error())
	}
	return
}

func (conn *Connection) readAuthResponse(r io.Reader) (err error) {
	respBytes, err := conn.read(r)
	if err != nil {
		return errors.New("auth: read error " + err.Error())
	}
	resp := Response{buf: smallBuf{b: respBytes}}
	err = resp.decodeHeader(conn.dec)
	if err != nil {
		return errors.New("auth: decode response header error " + err.Error())
	}
	err = resp.decodeBody()
	if err != nil {
		switch err.(type) {
		case Error:
			return err
		default:
			return errors.New("auth: decode response body error " + err.Error())
		}
	}
	return
}

func (conn *Connection) createConnection() (r *bufio.Reader, w *bufio.Writer, err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.closed {
		err = ClientError{ErrConnectionClosed, "using closed connection"}
		return
	}
	if conn.c == nil {
		var reconnects uint
		for {
			err = conn.dial()
			if err == nil {
				break
			} else if conn.opts.Reconnect > 0 {
				if conn.opts.MaxReconnects > 0 && reconnects > conn.opts.MaxReconnects {
					log.Printf("tarantool: last reconnect to %s failed: %s, giving it up.\n", conn.addr, err.Error())
					err = ClientError{ErrConnectionClosed, "last reconnect failed"}
					// mark connection as closed to avoid reopening by another goroutine
					return
				}
				log.Printf("tarantool: reconnect (%d/%d) to %s failed: %s\n", reconnects, conn.opts.MaxReconnects, conn.addr, err.Error())
				reconnects++
				time.Sleep(conn.opts.Reconnect)
				continue
			} else {
				return
			}
		}
	}
	r = conn.r
	w = conn.w
	return
}

func (conn *Connection) closeConnection(neterr error, r *bufio.Reader, w *bufio.Writer) (rr *bufio.Reader, ww *bufio.Writer, err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return
	}
	if w != nil && w != conn.w {
		return nil, conn.w, nil
	}
	if r != nil && r != conn.r {
		return conn.r, nil, nil
	}
	err = conn.c.Close()
	conn.lockShards()
	conn.c = nil
	conn.unlockShards()
	conn.r = nil
	conn.w = nil
	conn.lockShards()
	defer conn.unlockShards()
	for i := range conn.shard {
		conn.shard[i].buf = conn.shard[i].buf[:0]
		requests := conn.shard[i].requests
		for pos, pair := range requests {
			fut := pair.first
			requests[pos].first = nil
			requests[pos].last = &requests[pos].first
			for fut != nil {
				fut.err = neterr
				fut.markReady()
				fut, fut.next = fut.next, nil
			}
		}
	}
	return
}

func (conn *Connection) lockShards() {
	for i := range conn.shard {
		conn.shard[i].rmut.Lock()
		conn.shard[i].bufmut.Lock()
	}
}

func (conn *Connection) unlockShards() {
	for i := range conn.shard {
		conn.shard[i].rmut.Unlock()
		conn.shard[i].bufmut.Unlock()
	}
}

func (conn *Connection) closeConnectionForever(err error) error {
	conn.lockShards()
	conn.closed = true
	conn.unlockShards()
	close(conn.control)
	_, _, err = conn.closeConnection(err, nil, nil)
	return err
}

func (conn *Connection) writer() {
	var w *bufio.Writer
	var err error
	var shardn uint32
	var packet smallWBuf
	for !conn.closed {
		select {
		case shardn = <-conn.dirtyShard:
		default:
			runtime.Gosched()
			if len(conn.dirtyShard) == 0 && w != nil {
				if err := w.Flush(); err != nil {
					_, w, _ = conn.closeConnection(err, nil, w)
				}
			}
			select {
			case shardn = <-conn.dirtyShard:
			case <-conn.control:
				return
			}
		}
		if w == nil {
			if _, w, err = conn.createConnection(); err != nil {
				conn.closeConnectionForever(err)
				return
			}
		}
		shard := &conn.shard[shardn]
		shard.bufmut.Lock()
		packet, shard.buf = shard.buf, packet
		shard.bufmut.Unlock()
		if len(packet) == 0 {
			continue
		}
		if err := write(w, packet); err != nil {
			_, w, _ = conn.closeConnection(err, nil, w)
			continue
		}
		packet = packet[0:0]
	}
}

func (conn *Connection) reader() {
	var r *bufio.Reader
	var err error
	for !conn.closed {
		if r == nil {
			if r, _, err = conn.createConnection(); err != nil {
				conn.closeConnectionForever(err)
				return
			}
		}
		respBytes, err := conn.read(r)
		if err != nil {
			r, _, _ = conn.closeConnection(err, r, nil)
			continue
		}
		resp := &Response{buf: smallBuf{b: respBytes}}
		err = resp.decodeHeader(conn.dec)
		if err != nil {
			r, _, _ = conn.closeConnection(err, r, nil)
			continue
		}
		if fut := conn.fetchFuture(resp.RequestId); fut != nil {
			fut.resp = resp
			fut.markReady()
		} else {
			log.Printf("tarantool: unexpected requestId (%d) in response", uint(resp.RequestId))
		}
	}
}

func (conn *Connection) newFuture(requestCode int32) (fut *Future) {
	fut = &Future{}
	if conn.rlimit != nil && conn.opts.RLimitAction == RLimitDrop {
		select {
		case conn.rlimit <- struct{}{}:
		default:
			fut.err = ClientError{ErrRateLimited, "Request is rate limited on client"}
			return
		}
	}
	fut.ready = make(chan struct{})
	fut.conn = conn
	fut.requestId = conn.nextRequestId()
	fut.requestCode = requestCode
	shardn := fut.requestId & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardn]
	shard.rmut.Lock()
	if conn.closed {
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		close(fut.ready)
		shard.rmut.Unlock()
		return
	}
	if c := conn.c; c == nil {
		fut.err = ClientError{ErrConnectionNotReady, "client connection is not ready"}
		close(fut.ready)
		shard.rmut.Unlock()
		return fut
	}
	pos := (fut.requestId / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	*pair.last = fut
	pair.last = &fut.next
	if conn.opts.Timeout > 0 {
		fut.timeout = time.Now().Sub(epoch) + conn.opts.Timeout
	}
	shard.rmut.Unlock()
	if conn.rlimit != nil && conn.opts.RLimitAction == RLimitWait {
		select {
		case conn.rlimit <- struct{}{}:
		default:
			runtime.Gosched()
			select {
			case conn.rlimit <- struct{}{}:
			case <-fut.ready:
				if fut.err == nil {
					panic("fut.ready is closed, but err is nil")
				}
			}
		}
	}
	return
}

func (conn *Connection) putFuture(fut *Future, body func(*msgpack.Encoder) error) {
	shardn := fut.requestId & (conn.opts.Concurrency - 1)
	shard := &conn.shard[shardn]
	shard.bufmut.Lock()
	select {
	case <-fut.ready:
		shard.bufmut.Unlock()
		return
	default:
	}
	firstWritten := len(shard.buf) == 0
	if cap(shard.buf) == 0 {
		shard.buf = make(smallWBuf, 0, 128)
		shard.enc = msgpack.NewEncoder(&shard.buf)
	}
	blen := len(shard.buf)
	if err := fut.pack(&shard.buf, shard.enc, body); err != nil {
		shard.buf = shard.buf[:blen]
		fut.err = err
		shard.bufmut.Unlock()
		return
	}
	shard.bufmut.Unlock()
	if firstWritten {
		conn.dirtyShard <- shardn
	}
}

func (conn *Connection) fetchFuture(reqid uint32) (fut *Future) {
	shard := &conn.shard[reqid&(conn.opts.Concurrency-1)]
	shard.rmut.Lock()
	fut = conn.fetchFutureImp(reqid)
	shard.rmut.Unlock()
	return fut
}

func (conn *Connection) fetchFutureImp(reqid uint32) *Future {
	shard := &conn.shard[reqid&(conn.opts.Concurrency-1)]
	pos := (reqid / conn.opts.Concurrency) & (requestsMap - 1)
	pair := &shard.requests[pos]
	root := &pair.first
	for {
		fut := *root
		if fut == nil {
			return nil
		}
		if fut.requestId == reqid {
			*root = fut.next
			if fut.next == nil {
				pair.last = root
			} else {
				fut.next = nil
			}
			return fut
		}
		root = &fut.next
	}
}

func (conn *Connection) timeouts() {
	timeout := conn.opts.Timeout
	t := time.NewTimer(timeout)
	for {
		var nowepoch time.Duration
		select {
		case <-conn.control:
			t.Stop()
			return
		case <-t.C:
		}
		minNext := time.Now().Sub(epoch) + timeout
		for i := range conn.shard {
			nowepoch = time.Now().Sub(epoch)
			shard := &conn.shard[i]
			for pos := range shard.requests {
				shard.rmut.Lock()
				pair := &shard.requests[pos]
				for pair.first != nil && pair.first.timeout < nowepoch {
					shard.bufmut.Lock()
					fut := pair.first
					pair.first = fut.next
					if fut.next == nil {
						pair.last = &pair.first
					} else {
						fut.next = nil
					}
					fut.err = ClientError{
						Code: ErrTimeouted,
						Msg:  fmt.Sprintf("client timeout for request %d", fut.requestId),
					}
					fut.markReady()
					shard.bufmut.Unlock()
				}
				if pair.first != nil && pair.first.timeout < minNext {
					minNext = pair.first.timeout
				}
				shard.rmut.Unlock()
			}
		}
		nowepoch = time.Now().Sub(epoch)
		if nowepoch+time.Microsecond < minNext {
			t.Reset(minNext - nowepoch)
		} else {
			t.Reset(time.Microsecond)
		}
	}
}

func write(w io.Writer, data []byte) (err error) {
	l, err := w.Write(data)
	if err != nil {
		return
	}
	if l != len(data) {
		panic("Wrong length writed")
	}
	return
}

func (conn *Connection) read(r io.Reader) (response []byte, err error) {
	var length int

	if _, err = io.ReadFull(r, conn.lenbuf[:]); err != nil {
		return
	}
	if conn.lenbuf[0] != 0xce {
		err = errors.New("Wrong reponse header")
		return
	}
	length = (int(conn.lenbuf[1]) << 24) +
		(int(conn.lenbuf[2]) << 16) +
		(int(conn.lenbuf[3]) << 8) +
		int(conn.lenbuf[4])

	if length == 0 {
		err = errors.New("Response should not be 0 length")
		return
	}
	response = make([]byte, length)
	_, err = io.ReadFull(r, response)

	return
}

func (conn *Connection) nextRequestId() (requestId uint32) {
	return atomic.AddUint32(&conn.requestId, 1)
}

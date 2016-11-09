package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"gopkg.in/vmihailenco/msgpack.v2"
	"io"
	"log"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"time"
)

const shards = 32
const requestsMap = 32

var epoch = time.Now()

type connShard struct {
	rmut     sync.Mutex
	requests [requestsMap]struct{ first, last *Future }
	first    *Future
	last     **Future
	bufmut   sync.Mutex
	buf      smallWBuf
	enc      *msgpack.Encoder
	_pad     [16]uint64
}

type Connection struct {
	addr      string
	c         *net.TCPConn
	r         *bufio.Reader
	w         *bufio.Writer
	mutex     sync.Mutex
	Schema    *Schema
	requestId uint32
	Greeting  *Greeting

	shard      [shards]connShard
	dirtyShard chan uint32

	control chan struct{}
	opts    Opts
	closed  bool
	dec     *msgpack.Decoder
	lenbuf [PacketLengthBytes]byte
}

type Greeting struct {
	Version string
	auth    string
}

type Opts struct {
	Timeout       time.Duration // milliseconds
	Reconnect     time.Duration // milliseconds
	MaxReconnects uint
	User          string
	Pass          string
}

func Connect(addr string, opts Opts) (conn *Connection, err error) {

	conn = &Connection{
		addr:       addr,
		requestId:  0,
		Greeting:   &Greeting{},
		dirtyShard: make(chan uint32, shards),
		control:    make(chan struct{}),
		opts:       opts,
		dec:        msgpack.NewDecoder(&smallBuf{}),
	}
	for i := range conn.shard {
		conn.shard[i].last = &conn.shard[i].first
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
	go conn.timeouts()

	// TODO: reload schema after reconnect
	if err = conn.loadSchema(); err != nil {
		conn.closeConnection(err, nil, nil)
		return nil, err
	}

	return conn, err
}

func (conn *Connection) Close() error {
	err := ClientError{ErrConnectionClosed, "connection closed by client"}
	return conn.closeConnectionForever(err)
}

func (conn *Connection) RemoteAddr() string {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.c == nil {
		return ""
	}
	return conn.c.RemoteAddr().String()
}

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
	conn.c = c
	conn.r = r
	conn.w = w

	return
}

func (conn *Connection) writeAuthRequest(w *bufio.Writer, scramble []byte) (err error) {
	request := conn.newFuture(AuthRequest)
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
					conn.lockShards()
					conn.closed = true
					conn.unlockShards()
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
	conn.c = nil
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
			requests[pos].last = nil
			for fut != nil {
				fut.err = neterr
				close(fut.ready)
				fut, fut.next = fut.next, nil
			}
		}
	}
	return
}

func (conn *Connection) lockShards() {
	for i := range conn.shard {
		conn.shard[i].bufmut.Lock()
		conn.shard[i].rmut.Lock()
	}
}

func (conn *Connection) unlockShards() {
	for i := range conn.shard {
		conn.shard[i].bufmut.Unlock()
		conn.shard[i].rmut.Unlock()
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
Main:
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
			close(fut.ready)
		} else {
			log.Printf("tarantool: unexpected requestId (%d) in response", uint(resp.RequestId))
		}
	}
}

func (conn *Connection) putFuture(fut *Future, body func(*msgpack.Encoder) error) {
	shardn := fut.requestId & (shards - 1)
	shard := &conn.shard[shardn]
	shard.bufmut.Lock()
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
	shard.rmut.Lock()
	if conn.closed {
		shard.buf = shard.buf[:blen]
		shard.bufmut.Unlock()
		shard.rmut.Unlock()
		fut.err = ClientError{ErrConnectionClosed, "using closed connection"}
		return
	}
	shard.bufmut.Unlock()
	pos := (fut.requestId / shards) & (requestsMap - 1)
	pair := &shard.requests[pos]
	if pair.last == nil {
		shard.requests[pos].first = fut
	} else {
		shard.requests[pos].last.next = fut
	}
	shard.requests[pos].last = fut
	if conn.opts.Timeout > 0 {
		fut.timeout = time.Now().Sub(epoch) + conn.opts.Timeout
		fut.time.prev = shard.last
		*shard.last = fut
		shard.last = &fut.time.next
	}
	shard.rmut.Unlock()
	if firstWritten {
		conn.dirtyShard <- shardn
	}
}

func (conn *Connection) unlinkFutureTime(fut *Future) {
	if fut.time.prev != nil {
		*fut.time.prev = fut.time.next
		if fut.time.next != nil {
			fut.time.next.time.prev = fut.time.prev
		} else {
			i := fut.requestId & (shards - 1)
			conn.shard[i].last = fut.time.prev
		}
		fut.time.next = nil
		fut.time.prev = nil
	}
}

func (conn *Connection) fetchFuture(reqid uint32) (fut *Future) {
	shard := &conn.shard[reqid&(shards-1)]
	shard.rmut.Lock()
	fut = conn.fetchFutureImp(reqid)
	shard.rmut.Unlock()
	return fut
}

func (conn *Connection) fetchFutureImp(reqid uint32) *Future {
	shard := &conn.shard[reqid&(shards-1)]
	pos := (reqid / shards) & (requestsMap - 1)
	pair := &shard.requests[pos]
	fut := pair.first
	if fut == nil {
		return nil
	}
	if fut.requestId == reqid {
		pair.first = fut.next
		if pair.last == fut {
			pair.last = nil
		}
		conn.unlinkFutureTime(fut)
		return fut
	}
	for fut.next != nil {
		next := fut.next
		if next.requestId == reqid {
			if pair.last == next {
				pair.last = fut
			}
			fut, fut.next = next, next.next
			fut.next = nil
			conn.unlinkFutureTime(fut)
			return fut
		}
		fut = next
	}
	return nil
}

func (conn *Connection) timeouts() {
	timeout := conn.opts.Timeout
	if timeout == 0 {
		timeout = time.Second
	}
	t := time.NewTimer(timeout)
	for {
		var nowepoch time.Duration
		select {
		case <-conn.control:
			t.Stop()
			return
		case now := <-t.C:
			nowepoch = now.Sub(epoch)
		}
		minNext := nowepoch + timeout
		for i := range conn.shard {
			shard := &conn.shard[i]
			shard.rmut.Lock()
			for shard.first != nil && shard.first.timeout < nowepoch {
				fut := shard.first
				shard.rmut.Unlock()
				fut.timeouted()
				shard.rmut.Lock()
			}
			if shard.first != nil && shard.first.timeout < minNext {
				minNext = shard.first.timeout
			}
			shard.rmut.Unlock()
		}
		t.Reset(minNext - time.Now().Sub(epoch))
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

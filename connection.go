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

const requestsMap = 32*1024
var epoch = time.Now()

type Connection struct {
	addr      string
	c         *net.TCPConn
	r         *bufio.Reader
	w         *bufio.Writer
	mutex     sync.Mutex
	reqmut    sync.Mutex
	Schema    *Schema
	requestId uint32
	Greeting  *Greeting
	requests  []*Future
	time      struct {
		timeout time.Duration
		first *Future
		last  **Future
	}
	packets   chan []byte
	control   chan struct{}
	opts      Opts
	closed    bool
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
		addr:      addr,
		requestId: 0,
		Greeting:  &Greeting{},
		requests:  make([]*Future, requestsMap),
		packets:   make(chan []byte, 2048),
		control:   make(chan struct{}),
		opts:      opts,
	}
	conn.time.timeout = time.Now().Sub(epoch) + conn.opts.Timeout
	conn.time.last = &conn.time.first

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
	packet, err := request.pack(func(enc *msgpack.Encoder) error {
		return enc.Encode(map[uint32]interface{} {
			KeyUserName: conn.opts.User,
			KeyTuple: []interface{}{string("chap-sha1"), string(scramble)},
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
	respBytes, err := read(r)
	if err != nil {
		return errors.New("auth: read error " + err.Error())
	}
	resp := Response{buf: smallBuf{b: respBytes}}
	err = resp.decodeHeader()
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
					conn.closed = true
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
	conn.reqmut.Lock()
	defer conn.reqmut.Unlock()
	for pos, fut := range conn.requests {
		conn.requests[pos] = nil
		for fut != nil {
			fut.err = neterr
			close(fut.ready)
			fut, fut.next = fut.next, nil
		}
	}
	return
}

func (conn *Connection) closeConnectionForever(err error) error {
	conn.reqmut.Lock()
	conn.closed = true
	conn.reqmut.Unlock()
	close(conn.control)
	_, _, err = conn.closeConnection(err, nil, nil)
	return err
}

func (conn *Connection) writer() {
	var w *bufio.Writer
	var err error
	for !conn.closed {
		var packet []byte
		select {
		case packet = <-conn.packets:
		default:
			runtime.Gosched()
			if len(conn.packets) == 0 && w != nil {
				if err := w.Flush(); err != nil {
					_, w, _ = conn.closeConnection(err, nil, w)
				}
			}
			select {
			case packet = <-conn.packets:
			case <-conn.control:
				return
			}
		}
		if packet == nil {
			return
		}
		if w == nil {
			if _, w, err = conn.createConnection(); err != nil {
				conn.closeConnectionForever(err)
				return
			}
		}
		if err := write(w, packet); err != nil {
			_, w, _ = conn.closeConnection(err, nil, w)
			continue
		}
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
		respBytes, err := read(r)
		if err != nil {
			r, _, _ = conn.closeConnection(err, r, nil)
			continue
		}
		resp := Response{buf: smallBuf{b: respBytes}}
		err = resp.decodeHeader()
		if err != nil {
			r, _, _ = conn.closeConnection(err, r, nil)
			continue
		}
		conn.reqmut.Lock()
		if fut := conn.fetchFuture(resp.RequestId); fut != nil {
			fut.resp = resp
			close(fut.ready)
			conn.reqmut.Unlock()
		} else {
			conn.reqmut.Unlock()
			log.Printf("tarantool: unexpected requestId (%d) in response", uint(resp.RequestId))
		}
	}
}

func (conn *Connection) putFuture(fut *Future) {
	pos := fut.requestId & (requestsMap-1)
	fut.next = conn.requests[pos]
	conn.requests[pos] = fut
	if conn.opts.Timeout > 0 {
		fut.timeout = conn.time.timeout
		fut.time.prev = conn.time.last
		*conn.time.last = fut
		conn.time.last = &fut.time.next
	}
}

func (conn *Connection) unlinkFutureTime(fut *Future) {
	if fut.time.prev != nil {
		*fut.time.prev = fut.time.next
		if fut.time.next != nil {
			fut.time.next.time.prev = fut.time.prev
		} else {
			conn.time.last = fut.time.prev
		}
	}
}

func (conn *Connection) fetchFuture(reqid uint32) *Future {
	pos := reqid & (requestsMap-1)
	fut := conn.requests[pos]
	if fut == nil {
		return nil
	}
	if fut.requestId == reqid {
		conn.requests[pos] = fut.next
		conn.unlinkFutureTime(fut)
		return fut
	}
	for fut.next != nil {
		if fut.next.requestId == reqid {
			fut, fut.next = fut.next, fut.next.next
			conn.unlinkFutureTime(fut)
			return fut
		}
		fut = fut.next
	}
	return nil
}

func (conn *Connection) timeouts() {
	t := time.NewTicker(1*time.Millisecond)
	for {
		var nowepoch time.Duration
		select {
		case <-conn.control:
			t.Stop()
			return
		case now := <-t.C:
			nowepoch = now.Sub(epoch)
		}
		conn.reqmut.Lock()
		conn.time.timeout = nowepoch + conn.opts.Timeout
		for conn.time.first != nil && conn.time.first.timeout < nowepoch {
			fut := conn.time.first
			conn.reqmut.Unlock()
			fut.timeouted()
			conn.reqmut.Lock()
		}
		conn.reqmut.Unlock()
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

func read(r io.Reader) (response []byte, err error) {
	var lenbuf [PacketLengthBytes]byte
	var length int

	if _, err = io.ReadFull(r, lenbuf[:]); err != nil {
		return
	}
	if lenbuf[0] != 0xce {
		err = errors.New("Wrong reponse header")
		return
	}
	length = (int(lenbuf[1]) << 24) +
		(int(lenbuf[2]) << 16) +
		(int(lenbuf[3]) << 8) +
		int(lenbuf[4])

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

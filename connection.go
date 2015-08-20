package tarantool

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"sync/atomic"
	"time"
	"crypto/sha1"
	"encoding/base64"
)

type Connection struct {
	addr       string
	connection net.Conn
	r          io.Reader
	w          *bufio.Writer
	mutex      *sync.Mutex
	requestId  uint32
	Greeting   *Greeting
	requests   map[uint32]*Future
	packets    chan []byte
	control    chan struct{}
	opts       Opts
	closed     bool
}

type Greeting struct {
	version string
	auth    string
}

type Opts struct {
	Timeout   time.Duration // milliseconds
	Reconnect time.Duration // milliseconds
	User      string
	Pass      string
}

func Connect(addr string, opts Opts) (conn *Connection, err error) {

	conn = &Connection{
		addr:       addr,
		connection: nil,
		mutex:      &sync.Mutex{},
		requestId:  0,
		Greeting:   &Greeting{},
		requests:   make(map[uint32]*Future),
		packets:    make(chan []byte, 64),
		control:    make(chan struct{}),
		opts:       opts,
	}

	err = conn.dial()
	if err != nil {
		return
	}

	go conn.writer()
	go conn.reader()

	// Send auth request if needed
	if opts.User != "" {
		err = conn.auth()
		if err != nil {
			return
		}
	}

	return
}

func (conn *Connection) Close() (err error) {
	conn.closed = true
	close(conn.control)
	err = conn.closeConnection(errors.New("client closed connection"))
	return
}

func (conn *Connection) dial() (err error) {
	connection, err := net.Dial("tcp", conn.addr)
	if err != nil {
		return
	}
	connection.(*net.TCPConn).SetNoDelay(true)
	conn.connection = connection
	conn.r = bufio.NewReaderSize(conn.connection, 128*1024)
	conn.w = bufio.NewWriter(conn.connection)
	greeting := make([]byte, 128)
	// TODO: read all
	_, err = conn.connection.Read(greeting)
	if err != nil {
		return
	}
	conn.Greeting.version = bytes.NewBuffer(greeting[:64]).String()
	conn.Greeting.auth = bytes.NewBuffer(greeting[64:108]).String()
	return
}

func (conn *Connection) auth() (err error) {
	scr, err := scramble(conn.Greeting.auth, conn.opts.Pass)
	if err != nil {
		return
	}
	_, err = conn.Auth(conn.opts.User, []interface{}{string("chap-sha1"), string(scr)})
	return
}

func scramble(encoded_salt, pass string) (scramble []byte, err error) {
	/* ==================================================================
		Acording to: http://tarantool.org/doc/dev_guide/box-protocol.html

		salt = base64_decode(encoded_salt);
	    step_1 = sha1(password);
	    step_2 = sha1(step_1);
	    step_3 = sha1(salt, step_2);
	    scramble = xor(step_1, step_3);
	    return scramble;

	===================================================================== */
	scrambleSize := sha1.Size // == 20

    salt, err := base64.StdEncoding.DecodeString(encoded_salt)
	if err != nil {
	    return
	}
	step_1 := sha1.Sum([]byte(pass))
	step_2 := sha1.Sum(step_1[0:])
	hash := sha1.New() // may be create it once per connection ?
	hash.Write(salt[0:scrambleSize])
	hash.Write(step_2[0:])
	step_3 := hash.Sum(nil)
	
	return xor(step_1[0:], step_3[0:], scrambleSize), nil
}

func xor(left, right []byte, size int) []byte {
	result := make([]byte, size)
	for i := 0; i < size ; i++ {
		result[i] = left[i] ^ right[i]
	}
	return result
}

func (conn *Connection) createConnection() (r io.Reader, w *bufio.Writer) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	for conn.connection == nil {
		if conn.closed {
			return
		}
		err := conn.dial()
		if err == nil {
			break
		} else if conn.opts.Reconnect > 0 {
			time.Sleep(conn.opts.Reconnect)
		} else {
			return
		}
	}
	return conn.r, conn.w
}

func (conn *Connection) closeConnection(neterr error) (err error) {
	conn.mutex.Lock()
	defer conn.mutex.Unlock()
	if conn.connection == nil {
		return
	}
	err = conn.connection.Close()
	conn.connection = nil
	conn.r = nil
	conn.w = nil
	for rid, fut := range conn.requests {
		fut.err = neterr
		close(fut.c)
		delete(conn.requests, rid)
	}
	return
}

func (conn *Connection) writer() {
	var w *bufio.Writer
	for {
		var packet []byte
		select {
		case packet = <-conn.packets:
		default:
			if w = conn.w; w != nil {
				if err := w.Flush(); err != nil {
					conn.closeConnection(err)
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
		if w = conn.w; w == nil {
			if _, w = conn.createConnection(); w == nil {
				return
			}
		}
		if err := write(w, packet); err != nil {
			conn.closeConnection(err)
			continue
		}
	}
}

func (conn *Connection) reader() {
	var r io.Reader
	for {
		if r = conn.r; r == nil {
			if r, _ = conn.createConnection(); r == nil {
				return
			}
		}
		resp_bytes, err := read(r)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
		resp := Response{buf:smallBuf{b:resp_bytes}}
		err = resp.decodeHeader()
		//resp, err := newResponse(resp_bytes)
		if err != nil {
			conn.closeConnection(err)
			continue
		}
		conn.mutex.Lock()
		if fut, ok := conn.requests[resp.RequestId]; ok {
			delete(conn.requests, resp.RequestId)
			fut.resp = resp
			close(fut.c)
			conn.mutex.Unlock()
		} else {
			conn.mutex.Unlock()
			log.Printf("tarantool: unexpected requestId (%d) in response", uint(resp.RequestId))
		}
	}
}

func write(connection io.Writer, data []byte) (err error) {
	l, err := connection.Write(data)
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

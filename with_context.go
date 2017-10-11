package tarantool

import (
	"context"
	"errors"
)

type ConnWithContext struct {
	Conn *Connection
	Ctx  context.Context
}

func (conn *Connection) WithCtx(ctx context.Context) *ConnWithContext {
	return &ConnWithContext{
		Conn:      conn,
		Ctx:       ctx,
	}
}

var CtxError = errors.New("Context already closed")

func (cwc ConnWithContext) wait(fut *Future) {
	if fut.ready == nil {
		return
	}
	done := cwc.Ctx.Done()
	/* should we prefer failing on ctx.Done() ? or process good responses ? */
	select {
	case <-done:
	default:
		select {
		case <-fut.ready:
			return
		default:
			select {
			case <-fut.ready:
				return
			case <-done:
			}
		}
	}
	fut.fail(cwc.Conn, CtxError)
}

func (cwc ConnWithContext) Get(fut *Future) (*Response, error) {
	cwc.wait(fut)
	if fut.err != nil {
		return fut.resp, fut.err
	}
	fut.err = fut.resp.decodeBody()
	return fut.resp, fut.err
}

func (cwc ConnWithContext) GetTyped(fut *Future, result interface{}) error {
	cwc.wait(fut)
	if fut.err != nil {
		return fut.err
	}
	fut.err = fut.resp.decodeBodyTyped(result)
	return fut.err
}

func (cwc ConnWithContext) getSync(fgen func() *Future) (*Response, error) {
	select {
	case <-cwc.Ctx.Done():
		return nil, CtxError
	default:
		return cwc.Get(fgen())
	}
}

func (cwc ConnWithContext) getTypedSync(fgen func() *Future, result interface{}) error {
	select {
	case <-cwc.Ctx.Done():
		return CtxError
	default:
		return cwc.GetTyped(fgen(), result)
	}
}

var failedFuture = &Future{err: CtxError}

func (cwc ConnWithContext) getAsync(fgen func() *Future) *Future {
	select {
	case <-cwc.Ctx.Done():
		return failedFuture
	default:
		return fgen()
	}
}

func (cwc ConnWithContext) Select(space, index interface{}, offset, limit, iterator uint32, key interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.SelectAsync(space, index, offset, limit, iterator, key)
	})
}

func (cwc ConnWithContext) Insert(space, tuple interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.InsertAsync(space, tuple)
	})
}

func (cwc ConnWithContext) Replace(space, tuple interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.ReplaceAsync(space, tuple)
	})
}

func (cwc ConnWithContext) Delete(space, index, key interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.DeleteAsync(space, index, key)
	})
}

func (cwc ConnWithContext) Update(space, index, key, ops interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.UpdateAsync(space, index, key, ops)
	})
}

func (cwc ConnWithContext) Upsert(space, tuple, ops interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.UpsertAsync(space, tuple, ops)
	})
}

func (cwc ConnWithContext) Call(functionName string, args interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.CallAsync(functionName, args)
	})
}

func (cwc ConnWithContext) Call17(functionName string, args interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.Call17Async(functionName, args)
	})
}

func (cwc ConnWithContext) Eval(expr string, args interface{}) (*Response, error) {
	return cwc.getSync(func() *Future {
		return cwc.Conn.EvalAsync(expr, args)
	})
}

func (cwc ConnWithContext) SelectTyped(space, index interface{}, offset, limit, iterator uint32, key, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.SelectAsync(space, index, offset, limit, iterator, key)
	}, result)
}

func (cwc ConnWithContext) InsertTyped(space, tuple, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.InsertAsync(space, tuple)
	}, result)
}

func (cwc ConnWithContext) ReplaceTyped(space, tuple, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.ReplaceAsync(space, tuple)
	}, result)
}

func (cwc ConnWithContext) DeleteTyped(space, index, key, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.DeleteAsync(space, index, key)
	}, result)
}

func (cwc ConnWithContext) UpdateTyped(space, index, key, ops, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.UpdateAsync(space, index, key, ops)
	}, result)
}

func (cwc ConnWithContext) UpsertTyped(space, tuple, ops, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.UpsertAsync(space, tuple, ops)
	}, result)
}

func (cwc ConnWithContext) CallTyped(functionName string, args, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.CallAsync(functionName, args)
	}, result)
}

func (cwc ConnWithContext) Call17Typed(functionName string, args, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.Call17Async(functionName, args)
	}, result)
}

func (cwc ConnWithContext) EvalTyped(expr string, args, result interface{}) error {
	return cwc.getTypedSync(func() *Future {
		return cwc.Conn.EvalAsync(expr, args)
	}, result)
}

func (cwc ConnWithContext) SelectAsync(space, index interface{}, offset, limit, iterator uint32, key interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.SelectAsync(space, index, offset, limit, iterator, key)
	})
}

func (cwc ConnWithContext) InsertAsync(space, tuple interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.InsertAsync(space, tuple)
	})
}

func (cwc ConnWithContext) ReplaceAsync(space, tuple interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.ReplaceAsync(space, tuple)
	})
}

func (cwc ConnWithContext) DeleteAsync(space, index, key interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.DeleteAsync(space, index, key)
	})
}

func (cwc ConnWithContext) UpdateAsync(space, index, key, ops interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.UpdateAsync(space, index, key, ops)
	})
}

func (cwc ConnWithContext) UpsertAsync(space, tuple, ops interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.UpsertAsync(space, tuple, ops)
	})
}

func (cwc ConnWithContext) CallAsync(functionName string, args interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.CallAsync(functionName, args)
	})
}

func (cwc ConnWithContext) Call17Async(functionName string, args interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.Call17Async(functionName, args)
	})
}

func (cwc ConnWithContext) EvalAsync(expr string, args interface{}) *Future {
	return cwc.getAsync(func() *Future {
		return cwc.Conn.EvalAsync(expr, args)
	})
}

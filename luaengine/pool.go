package luaengine

import (
	"context"

	lua "github.com/yuin/gopher-lua"
)

type LuaPool interface {
	Get(ctx context.Context) (*lua.LState, error)
	Put(L *lua.LState)
}

type LStatePool struct {
	ch      chan *lua.LState
	newFunc func() *lua.LState
}

func NewLStatePool(size int, newFunc func() *lua.LState) *LStatePool {
	if size <= 0 {
		size = 1
	}
	if newFunc == nil {
		newFunc = func() *lua.LState { return lua.NewState() }
	}
	return &LStatePool{
		ch:      make(chan *lua.LState, size),
		newFunc: newFunc,
	}
}

func (p *LStatePool) Prewarm() {
	for {
		select {
		case p.ch <- p.newFunc():
		default:
			return
		}
	}
}

func (p *LStatePool) Get(ctx context.Context) (*lua.LState, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	select {
	case L := <-p.ch:
		return L, nil
	default:
		return p.newFunc(), nil
	}
}

func (p *LStatePool) Put(L *lua.LState) {
	if L == nil {
		return
	}
	L.SetTop(0)
	select {
	case p.ch <- L:
	default:
		L.Close()
	}
}


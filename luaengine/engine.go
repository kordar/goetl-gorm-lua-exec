package luaengine

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
	lua "github.com/yuin/gopher-lua"
)

type Injector func(L *lua.LState) error

type Engine struct {
	reg       *scriptstore.Registry
	injectors []Injector
	funcs     map[string]lua.LGFunction
	globals   map[string]any
	pool      LuaPool
}

func New() *Engine {
	return &Engine{}
}

func (e *Engine) WithRegistry(reg *scriptstore.Registry) *Engine {
	e.reg = reg
	return e
}

func (e *Engine) WithFunc(name string, fn lua.LGFunction) *Engine {
	if name == "" || fn == nil {
		return e
	}
	if e.funcs == nil {
		e.funcs = map[string]lua.LGFunction{}
	}
	e.funcs[name] = fn
	return e
}

func (e *Engine) WithFuncs(funcs map[string]lua.LGFunction) *Engine {
	for name, fn := range funcs {
		e.WithFunc(name, fn)
	}
	return e
}

func (e *Engine) WithGlobal(name string, v any) *Engine {
	if name == "" {
		return e
	}
	if e.globals == nil {
		e.globals = map[string]any{}
	}
	e.globals[name] = v
	return e
}

func (e *Engine) WithGlobals(globals map[string]any) *Engine {
	for name, v := range globals {
		e.WithGlobal(name, v)
	}
	return e
}

func (e *Engine) WithPool(pool LuaPool) *Engine {
	e.pool = pool
	return e
}

func (e *Engine) WithInjector(inj Injector) *Engine {
	if inj != nil {
		e.injectors = append(e.injectors, inj)
	}
	return e
}

func (e *Engine) ExecByRegistryName(ctx context.Context, key, name string, opts ExecOptions) (lua.LValue, error) {
	if e.reg == nil {
		return lua.LNil, errors.New("registry is nil")
	}
	sf, ok := e.reg.Find(key, name)
	if !ok {
		return lua.LNil, fmt.Errorf("script not found: key=%s name=%s", key, name)
	}
	return e.execScriptFile(ctx, sf, opts)
}

func (e *Engine) ExecByPath(ctx context.Context, path string, opts ExecOptions) (lua.LValue, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return lua.LNil, err
	}
	return e.ExecByContent(ctx, string(b), opts)
}

func (e *Engine) ExecByContent(ctx context.Context, content string, opts ExecOptions) (lua.LValue, error) {
	sf := scriptstore.ScriptFile{Content: content}
	return e.execScriptFile(ctx, sf, opts)
}

type ExecOptions struct {
	Globals map[string]any

	ProcessFunc string
	Payload     any
	Options     any
}

func (o *ExecOptions) normalize() {
	if o.ProcessFunc == "" {
		o.ProcessFunc = "process"
	}
}

func (e *Engine) execScriptFile(ctx context.Context, sf scriptstore.ScriptFile, opts ExecOptions) (lua.LValue, error) {
	opts.normalize()

	var (
		L   *lua.LState
		err error
	)
	if e.pool != nil {
		L, err = e.pool.Get(ctx)
		if err != nil {
			return lua.LNil, err
		}
		defer e.pool.Put(L)
	} else {
		L = lua.NewState()
		defer L.Close()
	}

	if err := ctx.Err(); err != nil {
		return lua.LNil, err
	}
	L.SetContext(ctx)

	code := sf.Content
	if code == "" && sf.Path != "" {
		b, err := os.ReadFile(sf.Path)
		if err != nil {
			return lua.LNil, err
		}
		code = string(b)
	}
	if code == "" {
		return lua.LNil, errors.New("script content is empty")
	}

	chunk, err := L.LoadString(code)
	if err != nil {
		return lua.LNil, err
	}

	env := L.NewTable()
	mt := L.NewTable()
	mt.RawSetString("__index", L.GetGlobal("_G"))
	L.SetMetatable(env, mt)

	for name, fn := range e.funcs {
		env.RawSetString(name, L.NewFunction(fn))
	}
	for name, v := range e.globals {
		env.RawSetString(name, toLValue(L, v))
	}
	for _, inj := range e.injectors {
		if err := inj(L); err != nil {
			return lua.LNil, err
		}
	}

	for k, v := range opts.Globals {
		env.RawSetString(k, toLValue(L, v))
	}
	if opts.Payload != nil {
		env.RawSetString("payload", toLValue(L, opts.Payload))
	}
	if opts.Options != nil {
		env.RawSetString("options", toLValue(L, opts.Options))
	}

	L.SetFEnv(chunk, env)

	if err := L.CallByParam(lua.P{Fn: chunk, NRet: 0, Protect: true}); err != nil {
		return lua.LNil, err
	}

	fn := env.RawGetString(opts.ProcessFunc)
	if fn == lua.LNil {
		return lua.LNil, nil
	}
	if fn.Type() != lua.LTFunction {
		return lua.LNil, fmt.Errorf("%s is not a function", opts.ProcessFunc)
	}

	if err := L.CallByParam(lua.P{
		Fn:      fn,
		NRet:    1,
		Protect: true,
	}, toLValue(L, opts.Payload), toLValue(L, opts.Options)); err != nil {
		return lua.LNil, err
	}

	ret := L.Get(-1)
	L.Pop(1)
	return ret, nil
}

package luaengine

import (
	"context"
	"testing"

	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
	lua "github.com/yuin/gopher-lua"
)

func TestEngine_ExecByContent_Process(t *testing.T) {
	e := New().WithFunc("add", func(L *lua.LState) int {
		a := L.CheckInt(1)
		b := L.CheckInt(2)
		L.Push(lua.LNumber(a + b))
		return 1
	})

	ret, err := e.ExecByContent(context.Background(), `
function process(payload, options)
  return add(payload.a, options.b)
end
`, ExecOptions{
		Payload: map[string]any{"a": 1},
		Options: map[string]any{"b": 2},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ret.Type() != lua.LTNumber || float64(ret.(lua.LNumber)) != 3 {
		t.Fatalf("unexpected ret: %v", ret)
	}
}

func TestEngine_ExecByRegistryName(t *testing.T) {
	reg := scriptstore.NewRegistry()
	reg.Set("k", []scriptstore.ScriptFile{
		{
			Key:     "k",
			Name:    "n1",
			Content: "function process(payload, options) return payload.x end",
		},
	})

	e := New().WithRegistry(reg)
	ret, err := e.ExecByRegistryName(context.Background(), "k", "n1", ExecOptions{
		Payload: map[string]any{"x": 7},
	})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ret.Type() != lua.LTNumber || float64(ret.(lua.LNumber)) != 7 {
		t.Fatalf("unexpected ret: %v", ret)
	}
}

func TestEngine_IsolatedGlobals(t *testing.T) {
	pool := NewLStatePool(2, func() *lua.LState {
		return lua.NewState()
	})
	pool.Prewarm()
	e := New().WithPool(pool)
	_, err := e.ExecByContent(context.Background(), `
function process(payload, options)
  if x == nil then
    x = 1
    return 1
  end
  return x
end
`, ExecOptions{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	ret2, err := e.ExecByContent(context.Background(), `
function process(payload, options)
  if x == nil then
    x = 2
    return 2
  end
  return x
end
`, ExecOptions{})
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	if ret2.Type() != lua.LTNumber || float64(ret2.(lua.LNumber)) != 2 {
		t.Fatalf("unexpected ret: %v", ret2)
	}
}

package luaengine

import (
	"fmt"
	"reflect"

	lua "github.com/yuin/gopher-lua"
)

func toLValue(L *lua.LState, v any) lua.LValue {
	if v == nil {
		return lua.LNil
	}
	switch x := v.(type) {
	case lua.LValue:
		return x
	case string:
		return lua.LString(x)
	case []byte:
		return lua.LString(string(x))
	case bool:
		return lua.LBool(x)
	case int:
		return lua.LNumber(x)
	case int8:
		return lua.LNumber(x)
	case int16:
		return lua.LNumber(x)
	case int32:
		return lua.LNumber(x)
	case int64:
		return lua.LNumber(x)
	case uint:
		return lua.LNumber(x)
	case uint8:
		return lua.LNumber(x)
	case uint16:
		return lua.LNumber(x)
	case uint32:
		return lua.LNumber(x)
	case uint64:
		return lua.LNumber(x)
	case float32:
		return lua.LNumber(x)
	case float64:
		return lua.LNumber(x)
	case map[string]any:
		t := L.NewTable()
		for k, vv := range x {
			t.RawSetString(k, toLValue(L, vv))
		}
		return t
	case []any:
		t := L.NewTable()
		for i, vv := range x {
			t.RawSetInt(i+1, toLValue(L, vv))
		}
		return t
	}

	rv := reflect.ValueOf(v)
	for rv.Kind() == reflect.Pointer {
		if rv.IsNil() {
			return lua.LNil
		}
		rv = rv.Elem()
	}

	switch rv.Kind() {
	case reflect.Map:
		if rv.Type().Key().Kind() != reflect.String {
			return lua.LString(fmt.Sprintf("%v", v))
		}
		t := L.NewTable()
		iter := rv.MapRange()
		for iter.Next() {
			k := iter.Key().String()
			t.RawSetString(k, toLValue(L, iter.Value().Interface()))
		}
		return t
	case reflect.Slice, reflect.Array:
		t := L.NewTable()
		n := rv.Len()
		for i := 0; i < n; i++ {
			t.RawSetInt(i+1, toLValue(L, rv.Index(i).Interface()))
		}
		return t
	case reflect.Struct:
		t := L.NewTable()
		rt := rv.Type()
		for i := 0; i < rv.NumField(); i++ {
			f := rt.Field(i)
			if f.PkgPath != "" {
				continue
			}
			t.RawSetString(f.Name, toLValue(L, rv.Field(i).Interface()))
		}
		return t
	case reflect.String:
		return lua.LString(rv.String())
	case reflect.Bool:
		return lua.LBool(rv.Bool())
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		return lua.LNumber(rv.Int())
	case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64, reflect.Uintptr:
		return lua.LNumber(rv.Uint())
	case reflect.Float32, reflect.Float64:
		return lua.LNumber(rv.Float())
	default:
		return lua.LString(fmt.Sprintf("%v", v))
	}
}


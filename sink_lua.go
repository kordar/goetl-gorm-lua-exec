package luaexec

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/kordar/goetl"
	lua "github.com/yuin/gopher-lua"
	"gorm.io/gorm"
)

type scriptRow struct {
	ID      int64  `gorm:"column:id"`
	SID     string `gorm:"column:sid"`
	Scripts string `gorm:"column:scripts"`
	Type    string `gorm:"column:type"`
	Options string `gorm:"column:options"`
	Deleted int    `gorm:"column:deleted"`
}

type LuaDispatchSink struct {
	DB               *gorm.DB
	ScriptTable      string
	ScriptType       string
	ScriptCacheTTL   time.Duration
	lastLoadAtBySID  map[string]time.Time
	cachedScriptByID map[string][]scriptRow
}

func (s *LuaDispatchSink) Name() string {
	return "lua_dispatch_sink"
}

func (s *LuaDispatchSink) Write(ctx context.Context, r *goetl.Record) error {
	if s.DB == nil {
		return fmt.Errorf("lua dispatch sink requires DB")
	}
	if s.ScriptTable == "" {
		s.ScriptTable = "vd_report_etl_source_script"
	}
	if s.ScriptType == "" {
		s.ScriptType = "lua"
	}
	if r == nil || r.Data == nil {
		return nil
	}
	raw, _ := r.Data["json"].(string)
	if raw == "" {
		return nil
	}

	var env JSONEnvelope
	if err := json.Unmarshal([]byte(raw), &env); err != nil {
		return err
	}
	if env.SID == "" {
		return nil
	}

	scripts, err := s.loadScripts(ctx, env.SID)
	if err != nil {
		return err
	}
	for _, sc := range scripts {
		if err := s.execLua(raw, env, sc); err != nil {
			return err
		}
	}
	return nil
}

func (s *LuaDispatchSink) Close(ctx context.Context) error {
	_ = ctx
	return nil
}

func (s *LuaDispatchSink) loadScripts(ctx context.Context, sid string) ([]scriptRow, error) {
	if s.ScriptCacheTTL > 0 {
		if s.lastLoadAtBySID == nil {
			s.lastLoadAtBySID = map[string]time.Time{}
		}
		if s.cachedScriptByID == nil {
			s.cachedScriptByID = map[string][]scriptRow{}
		}
		if at, ok := s.lastLoadAtBySID[sid]; ok && time.Since(at) < s.ScriptCacheTTL {
			return s.cachedScriptByID[sid], nil
		}
	}

	var scripts []scriptRow
	if err := s.DB.WithContext(ctx).Table(s.ScriptTable).
		Select("id, sid, scripts, type, options, deleted").
		Where("deleted = 0 AND sid = ? AND type = ?", sid, s.ScriptType).
		Order("id ASC").
		Find(&scripts).Error; err != nil {
		return nil, err
	}

	if s.ScriptCacheTTL > 0 {
		s.lastLoadAtBySID[sid] = time.Now()
		s.cachedScriptByID[sid] = scripts
	}
	return scripts, nil
}

func (s *LuaDispatchSink) execLua(raw string, env JSONEnvelope, script scriptRow) error {
	L := lua.NewState()
	defer L.Close()

	payload := toLuaValue(L, env.Data)
	options := map[string]any{}
	if script.Options != "" {
		_ = json.Unmarshal([]byte(script.Options), &options)
	}
	optionLV := toLuaValue(L, options)
	L.SetGlobal("raw_json", lua.LString(raw))
	L.SetGlobal("sid", lua.LString(env.SID))
	L.SetGlobal("payload", payload)
	L.SetGlobal("options", optionLV)

	if err := L.DoString(script.Scripts); err != nil {
		return err
	}

	fn := L.GetGlobal("process")
	if fn.Type() == lua.LTFunction {
		if err := L.CallByParam(lua.P{Fn: fn, NRet: 1, Protect: true}, payload, optionLV); err != nil {
			return err
		}
		L.Pop(1)
	}
	return nil
}

func toLuaValue(L *lua.LState, v any) lua.LValue {
	switch x := v.(type) {
	case nil:
		return lua.LNil
	case string:
		return lua.LString(x)
	case bool:
		if x {
			return lua.LTrue
		}
		return lua.LFalse
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
	case []any:
		t := L.NewTable()
		for i, item := range x {
			t.RawSetInt(i+1, toLuaValue(L, item))
		}
		return t
	case map[string]any:
		t := L.NewTable()
		for k, item := range x {
			t.RawSetString(k, toLuaValue(L, item))
		}
		return t
	default:
		return lua.LString(fmt.Sprintf("%v", x))
	}
}

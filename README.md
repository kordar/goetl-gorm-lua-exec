# goetl-gorm-lua-exec

一个基于 GORM + Lua（gopher-lua）的脚本执行与加载组件：

- 脚本来源：MySQL 表、目录中的 .lua 文件、直接传入脚本文本
- 脚本管理：按 key 管理脚本列表；脚本唯一性由 key + name 决定
- 脚本执行：通过 Lua VM 池复用 LState；每次执行使用隔离的 env，避免全局变量污染
- 扩展能力：可注册外部函数/全局变量供 Lua 脚本调用

## 数据表

见 [rule.md](file:///d:/Projects/GolangProjects/github.com/etl/goetl-gorm-lua-exec/rule.md)

## 核心包

- 脚本注册表（内存）
  - [scriptstore](file:///d:/Projects/GolangProjects/github.com/etl/goetl-gorm-lua-exec/scriptstore)
  - 结构：`map[key][]ScriptFile`
  - ScriptFile 字段：`Key/Name/Path/Content`
  - 唯一性：同一个 key 下，Name 唯一
- 加载器（将脚本加载进 Registry）
  - [registryloader](file:///d:/Projects/GolangProjects/github.com/etl/goetl-gorm-lua-exec/registryloader)
  - GORM 加载：从 `vd_report_etl_source` + `vd_report_etl_source_script` 加载
  - 目录加载：扫描目录 `.lua` 文件并加载（支持轮询刷新）
- Lua 执行引擎
  - [luaengine](file:///d:/Projects/GolangProjects/github.com/etl/goetl-gorm-lua-exec/luaengine)
  - 支持通过 RegistryName / Path / Content 执行
  - 支持注册外部函数（WithFunc）和全局变量（WithGlobal）
  - 支持 VM 池复用（WithPool）

## 快速开始

### 1) 创建 Registry 并加载脚本

从 MySQL 表加载（GORM）：

```go
db, _ := gorm.Open(mysql.Open(dsn), &gorm.Config{})
reg := scriptstore.NewRegistry()

ldr := registryloader.NewGormLoader().
    WithSourceTable("vd_report_etl_source").
    WithScriptTable("vd_report_etl_source_script")

_ = ldr.Load(ctx, db, reg)
```

从目录加载（.lua 文件）：

```go
reg := scriptstore.NewRegistry()
ldr := registryloader.NewDirLoader("./lua", "my_key").
    WithRecursive(true).
    WithPollInterval(5 * time.Second)

go func() { _ = ldr.Run(ctx, reg) }()
```

### 2) 创建 Lua Engine（高并发：启用 VM 池）

```go
pool := luaengine.NewLStatePool(100, func() *lua.LState {
    return lua.NewState()
})
pool.Prewarm()

eng := luaengine.New().
    WithRegistry(reg).
    WithPool(pool).
    WithFunc("add", func(L *lua.LState) int {
        a := L.CheckInt(1)
        b := L.CheckInt(2)
        L.Push(lua.LNumber(a + b))
        return 1
    }).
    WithGlobal("app", "goetl")
```

### 2.1) 注册类似 Redis 的 set/get（Lua 脚本可直接调用）

下面示例会在 Lua VM 中注册 `set(key, value)` 和 `get(key)` 两个函数，Lua 脚本可以直接调用：

```go
type KV struct {
    mu sync.RWMutex
    m  map[string]string
}

kv := &KV{m: map[string]string{}}

eng := luaengine.New().
    WithRegistry(reg).
    WithPool(pool).
    WithFunc("set", func(L *lua.LState) int {
        key := L.CheckString(1)
        val := L.CheckString(2)
        kv.mu.Lock()
        kv.m[key] = val
        kv.mu.Unlock()
        return 0
    }).
    WithFunc("get", func(L *lua.LState) int {
        key := L.CheckString(1)
        kv.mu.RLock()
        val, ok := kv.m[key]
        kv.mu.RUnlock()
        if !ok {
            L.Push(lua.LNil)
            return 1
        }
        L.Push(lua.LString(val))
        return 1
    })
```

Lua 脚本示例：

```lua
function process(payload, options)
  set("k", payload.value)
  return get("k")
end
```

### 3) 执行脚本

通过 key + name 从 Registry 获取脚本并执行：

```go
ret, err := eng.ExecByRegistryName(ctx, "my_key", "path/to/script", luaengine.ExecOptions{
    Payload: map[string]any{"a": 1},
    Options: map[string]any{"b": 2},
})
_ = ret
_ = err
```

通过文件路径执行：

```go
ret, err := eng.ExecByPath(ctx, "./lua/a.lua", luaengine.ExecOptions{})
```

通过脚本文本执行（动态脚本）：

```go
ret, err := eng.ExecByContent(ctx, "function process(payload, options) return 1 end", luaengine.ExecOptions{})
```

## 脚本更新（动态加载）

### 1) 通过 Registry 直接更新脚本内容

```go
reg.Add("my_key", scriptstore.ScriptFile{
    Key:     "my_key",
    Name:    "inline1",
    Content: "function process(payload, options) return 123 end",
})
```

如果你希望“覆盖式更新”，可用 `reg.Set(key, files)` 直接替换该 key 的全部脚本列表。

### 2) 通过目录加载器更新（推荐）

目录下新增/删除/修改 `.lua` 文件，然后依赖轮询刷新：

```go
ldr := registryloader.NewDirLoader("./lua", "my_key").
    WithRecursive(true).
    WithPollInterval(5 * time.Second)
go func() { _ = ldr.Run(ctx, reg) }()
```

### 3) 通过 GORM Loader 更新（推荐）

更新 `vd_report_etl_source_script.scripts` 字段后，重新调用 `GormLoader.Load(...)` 即可刷新 Registry。

## Lua 脚本约定

- 默认查找并调用 `process(payload, options)`（可通过 ExecOptions.ProcessFunc 修改）
- 引擎会注入：
  - `payload`：你的输入数据
  - `options`：执行参数
  - `Globals`：额外全局变量（ExecOptions.Globals）
- `WithFunc` 注册的函数可在 Lua 内直接调用

## 扩展层说明（Func / Global / Injector）

LuaEngine 的扩展分三层，建议按用途拆分：

### 1) Func：工具函数（Lua 可调用）

用途：

- 给 Lua 脚本提供 “能力函数”（例如：KV 操作、HTTP 请求、签名计算、字符串处理、JSON 编解码等）
- 一般是无状态或内部自己维护状态（并发安全由你实现）

接口：

- `WithFunc(name, lua.LGFunction)`
- `WithFuncs(map[string]lua.LGFunction)`

特点：

- 这是 “Lua 侧调用 Go” 的主要入口
- 函数会注入到每次执行的 env 中（不会污染其他执行）

示例（注册 add/set/get）：

```go
eng := luaengine.New().
    WithPool(pool).
    WithFunc("add", func(L *lua.LState) int {
        a := L.CheckInt(1)
        b := L.CheckInt(2)
        L.Push(lua.LNumber(a + b))
        return 1
    })
```

### 2) Global：配置/常量（Lua 侧直接读取）

用途：

- 注入一些 “配置类/常量类” 数据，供 Lua 脚本读取
- 例如：appName、env、feature flags、外部依赖地址等

接口：

- `WithGlobal(name, value)`
- `WithGlobals(map[string]any)`
- 以及每次执行级别的 `ExecOptions.Globals`

推荐做法：

- “全局配置” 用 `WithGlobal(s)` 注入（引擎级别、所有执行共享）
- “每条请求变量” 用 `ExecOptions.Globals` 注入（每次执行独立）

示例：

```go
eng := luaengine.New().
    WithGlobal("app", "goetl").
    WithGlobal("env", "prod")

ret, _ := eng.ExecByContent(ctx, `
function process(payload, options)
  return app .. "-" .. env
end
`, luaengine.ExecOptions{})
```

### 3) Injector：系统能力（初始化/扩展库）

用途：

- 更偏 “系统级初始化”，例如：
  - 一次性注册一组函数（批量）
  - 载入/扩展 Lua 标准库（或第三方 Lua 模块）
  - 注册复杂对象或设置 metatable

接口：

- `WithInjector(func(L *lua.LState) error)`

特点：

- Injector 运行在每次执行的 LState 上（在脚本执行前执行）
- 适合做 “复杂初始化”，但不建议把简单函数都放 Injector（Func/Global 更清晰）

示例（批量注入）：

```go
eng := luaengine.New().WithInjector(func(L *lua.LState) error {
    L.SetGlobal("now_ms", L.NewFunction(func(L *lua.LState) int {
        L.Push(lua.LNumber(time.Now().UnixMilli()))
        return 1
    }))
    return nil
})
```

示例（Redis 注入：Lua 内可用 redis_get/redis_set）：

```go
package luaengine

import (
    "context"

    "github.com/redis/go-redis/v9"
    lua "github.com/yuin/gopher-lua"
)

func RedisInjector(rdb *redis.Client) Injector {
    return func(L *lua.LState) error {
        L.SetGlobal("redis_get", L.NewFunction(func(L *lua.LState) int {
            key := L.CheckString(1)
            val, err := rdb.Get(context.Background(), key).Result()
            if err != nil {
                L.Push(lua.LNil)
                return 1
            }
            L.Push(lua.LString(val))
            return 1
        }))

        L.SetGlobal("redis_set", L.NewFunction(func(L *lua.LState) int {
            key := L.CheckString(1)
            value := L.CheckString(2)
            err := rdb.Set(context.Background(), key, value, 0).Err()
            if err != nil {
                L.Push(lua.LBool(false))
                return 1
            }
            L.Push(lua.LBool(true))
            return 1
        }))

        return nil
    }
}
```

Lua 脚本 demo：

```lua
function process(payload, options)
  local ok = redis_set("user:" .. payload.id, payload.name)
  return ok
end
```

只读场景（get）：

```lua
function process(payload, options)
  return redis_get("user:" .. payload.id)
end
```

无返回值怎么办：

- 当前引擎会调用 `process(payload, options)` 并取 1 个返回值；如果你不需要返回值，可以直接 `return nil`（或不写 return，Lua 默认返回 nil）

```lua
function process(payload, options)
  redis_set("user:" .. payload.id, payload.name)
  return nil
end
```

## 运行验证

```bash
go test ./...
go vet ./...
```

# goetl-gorm-lua-exec

基于 `goetl` + `goetl-gorm` 的规则执行程序：

- 数据源规则来自 `vd_report_etl_source`
- 按 `options` 控制刷新参数与游标参数
- 执行结果先封装为 JSON 分发
- 下游根据 JSON 中 `sid` 查询 `vd_report_etl_source_script`
- 执行 `scripts` 字段中的 Lua 脚本

## 对应 rule.md 表结构

- `vd_report_etl_source`
  - `sql`：数据源 SQL
  - `options`：刷新参数、游标参数
- `vd_report_etl_source_script`
  - `sid`：数据源 ID
  - `scripts`：Lua 脚本
  - `options`：Lua 参数

## options 建议字段

`vd_report_etl_source.options` 建议 JSON：

```json
{
  "refresh_interval_ms": 5000,
  "checkpoint_key": "rule_source:1",
  "cursor_field": "id",
  "cursor_type": "int64",
  "use_cursor": true
}
```

## Lua 脚本约定

脚本可直接使用全局变量：

- `raw_json`：分发 JSON 字符串
- `sid`：source id
- `payload`：行数据对象
- `options`：脚本 options（JSON 解析后的 table）

如果定义了 `process(payload, options)` 函数，会被自动调用。

## 代码入口

- `NewProgram(db, cfg)`：构建程序
- `Program.Run(ctx)`：启动运行

## 运行验证

```bash
go test ./...
go vet ./...
```

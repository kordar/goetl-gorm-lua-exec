package luaexec

import "time"

type ProgramConfig struct {
	SourceTable        string
	ScriptTable        string
	PollInterval       time.Duration
	QueueBuffer        int
	MinWorkers         int
	MaxWorkers         int
	InitialWorkers     int
	CheckpointTable    string
	CheckpointNS       string
	ScriptType         string
	ScriptCacheSeconds int64
}

func (c ProgramConfig) withDefaults() ProgramConfig {
	if c.SourceTable == "" {
		c.SourceTable = "vd_report_etl_source"
	}
	if c.ScriptTable == "" {
		c.ScriptTable = "vd_report_etl_source_script"
	}
	if c.PollInterval <= 0 {
		c.PollInterval = 2 * time.Second
	}
	if c.QueueBuffer <= 0 {
		c.QueueBuffer = 1024
	}
	if c.MaxWorkers <= 0 {
		c.MaxWorkers = 8
	}
	if c.MinWorkers < 0 {
		c.MinWorkers = 0
	}
	if c.InitialWorkers <= 0 {
		c.InitialWorkers = c.MaxWorkers
	}
	if c.CheckpointTable == "" {
		c.CheckpointTable = "etl_checkpoints"
	}
	if c.CheckpointNS == "" {
		c.CheckpointNS = "gorm_lua_exec"
	}
	if c.ScriptType == "" {
		c.ScriptType = "lua"
	}
	return c
}

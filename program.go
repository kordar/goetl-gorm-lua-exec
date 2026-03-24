package luaexec

import (
	"context"
	"time"

	"github.com/kordar/goetl"
	checkpointdb "github.com/kordar/goetl-gorm/checkpoint"
	"github.com/kordar/goetl/engine"
	"gorm.io/gorm"
)

type Program struct {
	Engine *engine.Engine
}

func NewProgram(db *gorm.DB, cfg ProgramConfig) (*Program, error) {
	cfg = cfg.withDefaults()

	cp := &checkpointdb.Store{
		DB:        db,
		TableName: cfg.CheckpointTable,
		Namespace: cfg.CheckpointNS,
	}

	src := &RuleDrivenSource{
		DB:           db,
		Store:        cp,
		SourceTable:  cfg.SourceTable,
		PollInterval: cfg.PollInterval,
	}

	sink := &LuaDispatchSink{
		DB:             db,
		ScriptTable:    cfg.ScriptTable,
		ScriptType:     cfg.ScriptType,
		ScriptCacheTTL: timeFromSeconds(cfg.ScriptCacheSeconds),
	}

	eng := engine.NewEngine(
		sink,
		engine.WithPipeline(goetl.NewPipeline(&JSONDispatchTransform{})),
		engine.WithCheckpoints(cp),
		engine.WithQueueBuffer(cfg.QueueBuffer),
		engine.WithWorkers(cfg.MinWorkers, cfg.MaxWorkers, cfg.InitialWorkers),
	)
	if err := eng.LoadSource("rule_source", src); err != nil {
		return nil, err
	}

	return &Program{Engine: eng}, nil
}

func (p *Program) Run(ctx context.Context) error {
	return p.Engine.Run(ctx)
}

func timeFromSeconds(v int64) (d time.Duration) {
	if v <= 0 {
		return 0
	}
	return time.Duration(v) * time.Second
}

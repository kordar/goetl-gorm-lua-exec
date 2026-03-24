package luaexec

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"time"

	"github.com/kordar/goetl"
	checkpointdb "github.com/kordar/goetl-gorm/checkpoint"
	gormsource "github.com/kordar/goetl-gorm/source"
	"gorm.io/gorm"
)

type sourceRow struct {
	ID      int64  `gorm:"column:id"`
	Name    string `gorm:"column:name"`
	SQL     string `gorm:"column:sql"`
	Options string `gorm:"column:options"`
	Deleted int    `gorm:"column:deleted"`
}

type sourceOptions struct {
	RefreshIntervalMs int64  `json:"refresh_interval_ms"`
	CheckpointKey     string `json:"checkpoint_key"`
	CursorField       string `json:"cursor_field"`
	CursorType        string `json:"cursor_type"`
	UseCursor         *bool  `json:"use_cursor"`
}

type sourceState struct {
	nextRun time.Time
}

type RuleDrivenSource struct {
	DB            *gorm.DB
	Store         *checkpointdb.Store
	SourceTable   string
	PollInterval  time.Duration
	DefaultCursor string

	mu    sync.Mutex
	state map[int64]*sourceState
}

func (s *RuleDrivenSource) Name() string {
	return "gorm_rule_source"
}

func (s *RuleDrivenSource) Start(ctx context.Context, out chan<- goetl.Message) error {
	if s.DB == nil {
		return fmt.Errorf("rule source requires DB")
	}
	if s.SourceTable == "" {
		s.SourceTable = "vd_report_etl_source"
	}
	if s.PollInterval <= 0 {
		s.PollInterval = 2 * time.Second
	}
	if s.DefaultCursor == "" {
		s.DefaultCursor = "id"
	}
	if s.state == nil {
		s.state = map[int64]*sourceState{}
	}

	if err := s.tick(ctx, out); err != nil {
		return err
	}

	t := time.NewTicker(s.PollInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			if err := s.tick(ctx, out); err != nil {
				return err
			}
		}
	}
}

func (s *RuleDrivenSource) tick(ctx context.Context, out chan<- goetl.Message) error {
	var defs []sourceRow
	if err := s.DB.WithContext(ctx).Table(s.SourceTable).
		Select("id, name, `sql`, options, deleted").
		Where("deleted = 0").
		Find(&defs).Error; err != nil {
		return err
	}

	now := time.Now()
	for _, def := range defs {
		opts := parseSourceOptions(def.Options)
		refresh := time.Duration(opts.RefreshIntervalMs) * time.Millisecond
		if refresh <= 0 {
			refresh = s.PollInterval
		}

		st := s.getState(def.ID)
		if now.Before(st.nextRun) {
			continue
		}

		if err := s.runOne(ctx, def, opts, out); err != nil {
			return err
		}
		st.nextRun = now.Add(refresh)
	}
	return nil
}

func (s *RuleDrivenSource) runOne(ctx context.Context, def sourceRow, opts sourceOptions, out chan<- goetl.Message) error {
	cursorField := opts.CursorField
	if cursorField == "" {
		cursorField = s.DefaultCursor
	}
	cursorType := opts.CursorType
	if cursorType == "" {
		cursorType = "int64"
	}
	useCursor := true
	if opts.UseCursor != nil {
		useCursor = *opts.UseCursor
	}
	checkpointKey := opts.CheckpointKey
	if checkpointKey == "" {
		checkpointKey = "rule_source:" + strconv.FormatInt(def.ID, 10)
	}

	switch cursorType {
	case "int64":
		sc := &gormsource.SQLScanner[int64]{
			DB:            s.DB,
			Store:         s.Store,
			CheckpointKey: checkpointKey,
			Codec:         gormsource.Int64CursorCodec{},
			BuildQuery: func(cctx context.Context, cursor int64) (string, []any, error) {
				_ = cctx
				if useCursor {
					return def.SQL, []any{cursor}, nil
				}
				return def.SQL, nil, nil
			},
			ExtractCursor: func(row map[string]any) (int64, error) {
				if !useCursor {
					return 0, nil
				}
				return toInt64(row[cursorField])
			},
			MapRow: func(row map[string]any) (map[string]any, error) {
				out := make(map[string]any, len(row)+2)
				for k, v := range row {
					out[k] = v
				}
				out["sid"] = strconv.FormatInt(def.ID, 10)
				out["source_name"] = def.Name
				return out, nil
			},
		}
		return sc.Start(ctx, out)
	default:
		sc := &gormsource.SQLScanner[string]{
			DB:            s.DB,
			Store:         s.Store,
			CheckpointKey: checkpointKey,
			Codec:         gormsource.StringCursorCodec{},
			BuildQuery: func(cctx context.Context, cursor string) (string, []any, error) {
				_ = cctx
				if useCursor {
					return def.SQL, []any{cursor}, nil
				}
				return def.SQL, nil, nil
			},
			ExtractCursor: func(row map[string]any) (string, error) {
				if !useCursor {
					return "", nil
				}
				return toString(row[cursorField]), nil
			},
			MapRow: func(row map[string]any) (map[string]any, error) {
				out := make(map[string]any, len(row)+2)
				for k, v := range row {
					out[k] = v
				}
				out["sid"] = strconv.FormatInt(def.ID, 10)
				out["source_name"] = def.Name
				return out, nil
			},
		}
		return sc.Start(ctx, out)
	}
}

func (s *RuleDrivenSource) getState(id int64) *sourceState {
	s.mu.Lock()
	defer s.mu.Unlock()
	st := s.state[id]
	if st == nil {
		st = &sourceState{}
		s.state[id] = st
	}
	return st
}

func parseSourceOptions(raw string) sourceOptions {
	if raw == "" {
		return sourceOptions{}
	}
	var out sourceOptions
	_ = json.Unmarshal([]byte(raw), &out)
	return out
}

func toString(v any) string {
	switch x := v.(type) {
	case string:
		return x
	case []byte:
		return string(x)
	case fmt.Stringer:
		return x.String()
	default:
		return fmt.Sprintf("%v", v)
	}
}

func toInt64(v any) (int64, error) {
	switch x := v.(type) {
	case int64:
		return x, nil
	case int:
		return int64(x), nil
	case int32:
		return int64(x), nil
	case int16:
		return int64(x), nil
	case int8:
		return int64(x), nil
	case uint64:
		return int64(x), nil
	case uint:
		return int64(x), nil
	case uint32:
		return int64(x), nil
	case uint16:
		return int64(x), nil
	case uint8:
		return int64(x), nil
	case []byte:
		return strconv.ParseInt(string(x), 10, 64)
	case string:
		return strconv.ParseInt(x, 10, 64)
	default:
		return 0, fmt.Errorf("unsupported int64 type: %T", v)
	}
}

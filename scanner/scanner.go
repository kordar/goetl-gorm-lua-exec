package scanner

import (
	"context"
	"encoding/json"
	"time"

	"github.com/kordar/goetl"
	gormsource "github.com/kordar/goetl-gorm/source"
	"github.com/kordar/goetl/checkpoint"
	"gorm.io/gorm"
)

type Cursor struct {
	TS time.Time
	ID int64
}

type CursorCodec struct{}

func (CursorCodec) Encode(v Cursor) (string, error) {
	b, err := json.Marshal(map[string]any{
		"ts": v.TS.UTC().Format(time.RFC3339Nano),
		"id": v.ID,
	})
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func (CursorCodec) Decode(s string) (Cursor, error) {
	if s == "" {
		return Cursor{}, nil
	}
	var m struct {
		TS string `json:"ts"`
		ID int64  `json:"id"`
	}
	if err := json.Unmarshal([]byte(s), &m); err != nil {
		return Cursor{}, err
	}
	ts, err := time.Parse(time.RFC3339Nano, m.TS)
	if err != nil {
		return Cursor{}, err
	}
	return Cursor{TS: ts, ID: m.ID}, nil
}

func (CursorCodec) Zero() Cursor { return Cursor{} }

var extract = func(row map[string]any) (Cursor, error) {
	ts := row["updated_at"].(time.Time)
	id := row["id"].(int64)
	return Cursor{TS: ts, ID: id}, nil
}

func buildSource(db *gorm.DB, cp checkpoint.Store, storeKey, sql string, topics []string) goetl.Source {
	build := func(ctx context.Context, c Cursor) (string, []any, error) {
		_ = ctx
		return sql, []any{c.TS, c.TS, c.ID}, nil
	}
	return &gormsource.SQLScanner[Cursor]{
		DB:            db,
		Store:         cp,
		CheckpointKey: storeKey,
		Codec:         CursorCodec{},
		BuildQuery:    build,
		ExtractCursor: extract,
		MapRow: func(row map[string]any) (map[string]any, error) {
			out := make(map[string]any, len(row)+1)
			for k, v := range row {
				out[k] = v
			}
			out["topics"] = topics
			return out, nil
		},
	}
}

func BuildOrderSource(db *gorm.DB, cp checkpoint.Store) goetl.Source {
	sql := `SELECT *, update_time as updated_at
FROM vd_despatch_order
WHERE update_time > ? OR (update_time = ? AND id > ?)
ORDER BY update_time ASC, id ASC
LIMIT 100`
	var def struct {
		SQL     string `gorm:"column:sql"`
		Options string `gorm:"column:options"`
	}
	_ = db.WithContext(context.Background()).
		Table("vd_report_etl_source").
		Select("`sql`, options").
		Where("deleted = 0 AND name = ?", "order").
		Order("id DESC").
		Take(&def).Error
	if def.SQL != "" {
		sql = def.SQL
	}

	topics := []string{"order"}
	scanner := buildSource(db, cp, "order", sql, topics)
	ticker := gormsource.NewSQLScannerTicker(
		scanner.(*gormsource.SQLScanner[Cursor]),
		time.Duration(1*time.Minute),
		time.Duration(5*time.Minute),
		false,
	)
	return ticker
}

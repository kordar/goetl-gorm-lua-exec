package registryloader

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
	"gorm.io/gorm"
)

type Source struct {
	ID      int64  `gorm:"column:id;primaryKey"`
	Name    string `gorm:"column:name"`
	SQL     string `gorm:"column:sql"`
	Options string `gorm:"column:options"`
	Deleted int8   `gorm:"column:deleted"`
}

type SourceScript struct {
	ID      int64  `gorm:"column:id;primaryKey"`
	SID     int64  `gorm:"column:sid"`
	Scripts string `gorm:"column:scripts"`
	Name    string `gorm:"column:name"`
	Path    string `gorm:"column:path"`
	Type    string `gorm:"column:type"`
	Options string `gorm:"column:options"`
	Deleted int8   `gorm:"column:deleted"`
}

type GormLoader struct {
	SourceTable string
	ScriptTable string

	KeyFromSource func(s Source) string
	Types         []string
}

func NewGormLoader() *GormLoader {
	return &GormLoader{
		SourceTable:   "vd_report_etl_source",
		ScriptTable:   "vd_report_etl_source_script",
		KeyFromSource: func(s Source) string { return s.Name },
		Types:         []string{"lua"},
	}
}

func (l *GormLoader) WithSourceTable(name string) *GormLoader {
	l.SourceTable = name
	return l
}

func (l *GormLoader) WithScriptTable(name string) *GormLoader {
	l.ScriptTable = name
	return l
}

func (l *GormLoader) Load(ctx context.Context, db *gorm.DB, reg *scriptstore.Registry) error {
	if db == nil {
		return errors.New("db is nil")
	}
	if reg == nil {
		return errors.New("registry is nil")
	}
	if l == nil {
		return errors.New("loader is nil")
	}
	if l.KeyFromSource == nil {
		l.KeyFromSource = func(s Source) string { return s.Name }
	}
	if l.SourceTable == "" {
		l.SourceTable = "vd_report_etl_source"
	}
	if l.ScriptTable == "" {
		l.ScriptTable = "vd_report_etl_source_script"
	}

	var sources []Source
	if err := db.WithContext(ctx).Table(l.SourceTable).
		Where("deleted = 0").
		Find(&sources).Error; err != nil {
		return fmt.Errorf("load sources: %w", err)
	}

	sidToKey := make(map[int64]string, len(sources))
	allKeys := make(map[string]struct{}, len(sources))
	for _, s := range sources {
		key := l.KeyFromSource(s)
		if key == "" {
			continue
		}
		sidToKey[s.ID] = key
		allKeys[key] = struct{}{}
	}

	scriptsQuery := db.WithContext(ctx).Table(l.ScriptTable).Where("deleted = 0")
	if len(l.Types) > 0 {
		scriptsQuery = scriptsQuery.Where("type IN ?", l.Types)
	}

	var scripts []SourceScript
	if err := scriptsQuery.Find(&scripts).Error; err != nil {
		return fmt.Errorf("load scripts: %w", err)
	}

	group := map[string][]scriptstore.ScriptFile{}
	for _, sc := range scripts {
		key := sidToKey[sc.SID]
		if key == "" {
			continue
		}
		group[key] = append(group[key], scriptstore.ScriptFile{
			Key:     key,
			Name:    sc.Name,
			Path:    sc.Path,
			Content: sc.Scripts,
		})
	}

	for key := range allKeys {
		files := group[key]
		sort.SliceStable(files, func(i, j int) bool {
			return files[i].Name < files[j].Name
		})
		reg.Set(key, files)
	}

	return nil
}

package registryloader

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
)

type DirLoader struct {
	Dir         string
	Key         string
	Recursive   bool
	PollInterval time.Duration
}

func NewDirLoader(dir, key string) *DirLoader {
	return &DirLoader{
		Dir:       dir,
		Key:       key,
		Recursive: true,
	}
}

func (l *DirLoader) WithRecursive(v bool) *DirLoader {
	l.Recursive = v
	return l
}

func (l *DirLoader) WithPollInterval(v time.Duration) *DirLoader {
	l.PollInterval = v
	return l
}

func (l *DirLoader) LoadOnce(ctx context.Context, reg *scriptstore.Registry) error {
	if l == nil {
		return errors.New("loader is nil")
	}
	if reg == nil {
		return errors.New("registry is nil")
	}
	dir := strings.TrimSpace(l.Dir)
	if dir == "" {
		return errors.New("dir is empty")
	}
	key := strings.TrimSpace(l.Key)
	if key == "" {
		return errors.New("key is empty")
	}

	base, err := filepath.Abs(dir)
	if err != nil {
		return err
	}

	var out []scriptstore.ScriptFile
	walkFn := func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}
		if d.IsDir() {
			if !l.Recursive && path != base {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(strings.ToLower(d.Name()), ".lua") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}

		rel, err := filepath.Rel(base, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		name := strings.TrimSuffix(rel, filepath.Ext(rel))
		out = append(out, scriptstore.ScriptFile{
			Key:     key,
			Name:    name,
			Path:    path,
			Content: string(b),
		})
		return nil
	}

	if err := filepath.WalkDir(base, walkFn); err != nil {
		return err
	}

	sort.SliceStable(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	reg.Set(key, out)
	return nil
}

func (l *DirLoader) Run(ctx context.Context, reg *scriptstore.Registry) error {
	if l == nil {
		return errors.New("loader is nil")
	}
	if l.PollInterval <= 0 {
		return l.LoadOnce(ctx, reg)
	}
	t := time.NewTicker(l.PollInterval)
	defer t.Stop()

	if err := l.LoadOnce(ctx, reg); err != nil {
		return err
	}

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			_ = l.LoadOnce(ctx, reg)
		}
	}
}


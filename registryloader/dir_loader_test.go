package registryloader

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/kordar/goetl-gorm-lua-exec/scriptstore"
)

func TestDirLoader_LoadOnce(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "a.lua"), []byte("print('a')"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "b.lua"), []byte("print('b')"), 0644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "c.txt"), []byte("no"), 0644); err != nil {
		t.Fatal(err)
	}

	reg := scriptstore.NewRegistry()
	ldr := NewDirLoader(dir, "k").WithRecursive(false)
	if err := ldr.LoadOnce(context.Background(), reg); err != nil {
		t.Fatalf("load: %v", err)
	}

	got := reg.Get("k")
	if len(got) != 2 {
		t.Fatalf("expected 2, got %d: %#v", len(got), got)
	}
	if got[0].Name != "a" || got[1].Name != "b" {
		t.Fatalf("unexpected names: %#v", got)
	}
	if got[0].Content == "" || got[1].Content == "" {
		t.Fatalf("expected content loaded: %#v", got)
	}
}


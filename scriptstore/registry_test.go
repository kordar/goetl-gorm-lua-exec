package scriptstore

import "testing"

func TestRegistry_SetGet(t *testing.T) {
	r := NewRegistry()
	r.Set("a", []ScriptFile{
		{Name: "s1", Path: "1.lua"},
		{Name: "s2", Path: "2.lua"},
		{Name: "s1", Path: "1.lua"},
		{Name: "inline1", Content: "print(1)"},
		{Name: "inline1", Content: "print(1)"},
		{Path: ""},
	})
	got := r.Get("a")
	if len(got) != 3 || got[0].Name != "s1" || got[1].Name != "s2" || got[2].Name != "inline1" {
		t.Fatalf("unexpected: %#v", got)
	}
}

func TestRegistry_AddRemove(t *testing.T) {
	r := NewRegistry()
	if ok := r.Add("k", ScriptFile{Name: "a", Path: "a.lua"}); !ok {
		t.Fatalf("expected add ok")
	}
	if ok := r.Add("k", ScriptFile{Name: "a", Path: "a.lua"}); ok {
		t.Fatalf("expected add dup false")
	}
	if ok := r.Add("k", ScriptFile{Name: "b", Content: "print(1)"}); !ok {
		t.Fatalf("expected add ok")
	}
	if ok := r.Add("k", ScriptFile{Name: "b", Content: "print(2)"}); ok {
		t.Fatalf("expected add dup false")
	}
	if ok := r.Remove("k", ScriptFile{Name: "missing"}); ok {
		t.Fatalf("expected remove missing false")
	}
	if ok := r.Remove("k", ScriptFile{Name: "a"}); !ok {
		t.Fatalf("expected remove ok")
	}
	if ok := r.Remove("k", ScriptFile{Name: "b"}); !ok {
		t.Fatalf("expected remove ok")
	}
	if got := r.Get("k"); len(got) != 0 {
		t.Fatalf("expected empty after remove, got %#v", got)
	}
}

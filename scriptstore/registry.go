package scriptstore

import "sync"

type Registry struct {
	mu sync.RWMutex
	m  map[string][]ScriptFile
}

type ScriptFile struct {
	Key     string
	Name    string
	Path    string
	Content string
}

func NewRegistry() *Registry {
	return &Registry{m: map[string][]ScriptFile{}}
}

func (r *Registry) Set(key string, files []ScriptFile) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.m == nil {
		r.m = map[string][]ScriptFile{}
	}

	uniq := make([]ScriptFile, 0, len(files))
	seen := map[string]struct{}{}
	for _, f := range files {
		if f.Name == "" {
			continue
		}
		if f.Path == "" && f.Content == "" {
			continue
		}
		dedupeKey := key + "\x00" + f.Name
		if _, ok := seen[dedupeKey]; ok {
			continue
		}
		seen[dedupeKey] = struct{}{}
		uniq = append(uniq, f)
	}

	if len(uniq) == 0 {
		delete(r.m, key)
		return
	}
	r.m[key] = uniq
}

func (r *Registry) Add(key string, file ScriptFile) bool {
	if file.Name == "" {
		return false
	}
	if file.Path == "" && file.Content == "" {
		return false
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if r.m == nil {
		r.m = map[string][]ScriptFile{}
	}

	cur := r.m[key]
	for _, f := range cur {
		if f.Name == file.Name {
			return false
		}
	}
	r.m[key] = append(cur, file)
	return true
}

func (r *Registry) Remove(key string, file ScriptFile) bool {
	if file.Name == "" {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	cur, ok := r.m[key]
	if !ok || len(cur) == 0 {
		return false
	}

	out := cur[:0]
	removed := false
	for _, f := range cur {
		if f.Name == file.Name {
			removed = true
			continue
		}
		out = append(out, f)
	}

	if !removed {
		return false
	}

	if len(out) == 0 {
		delete(r.m, key)
		return true
	}

	r.m[key] = out
	return true
}

func (r *Registry) Delete(key string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.m[key]; !ok {
		return false
	}
	delete(r.m, key)
	return true
}

func (r *Registry) Get(key string) []ScriptFile {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cur := r.m[key]
	out := make([]ScriptFile, len(cur))
	copy(out, cur)
	return out
}

func (r *Registry) Find(key, name string) (ScriptFile, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	cur := r.m[key]
	for _, f := range cur {
		if f.Name == name {
			return f, true
		}
	}
	return ScriptFile{}, false
}

func (r *Registry) Keys() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]string, 0, len(r.m))
	for k := range r.m {
		out = append(out, k)
	}
	return out
}

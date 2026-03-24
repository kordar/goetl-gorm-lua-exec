package luaexec

import (
	"context"
	"encoding/json"

	"github.com/kordar/goetl"
)

type JSONEnvelope struct {
	SID  string         `json:"sid"`
	Name string         `json:"name,omitempty"`
	Data map[string]any `json:"data"`
}

type JSONDispatchTransform struct{}

func (t *JSONDispatchTransform) Name() string {
	return "json_dispatch_transform"
}

func (t *JSONDispatchTransform) Transform(ctx context.Context, r *goetl.Record) (*goetl.Record, error) {
	_ = ctx
	if r == nil || r.Data == nil {
		return r, nil
	}
	sid, _ := r.Data["sid"].(string)
	name, _ := r.Data["source_name"].(string)
	payload := map[string]any{}
	for k, v := range r.Data {
		if k == "sid" || k == "source_name" {
			continue
		}
		payload[k] = v
	}
	env := JSONEnvelope{
		SID:  sid,
		Name: name,
		Data: payload,
	}
	b, err := json.Marshal(env)
	if err != nil {
		return nil, err
	}
	r.Data = map[string]any{
		"sid":  sid,
		"json": string(b),
	}
	return r, nil
}

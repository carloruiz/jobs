package jobs_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"github.com/carloruiz/jobs"
)

type addRequest struct {
	A int `json:"a"`
	B int `json:"b"`
}

type addResponse struct {
	Sum int `json:"sum"`
}

func newRuntime() *jobs.Runtime {
	return jobs.NewRuntime(nil, nil, jobs.Config{})
}

func TestRegister_RoundTrip(t *testing.T) {
	rt := newRuntime()
	jobs.Register(rt, "add", func(ctx context.Context, req addRequest) (addResponse, error) {
		return addResponse{Sum: req.A + req.B}, nil
	})

	h, ok := rt.Lookup("add")
	if !ok {
		t.Fatal("handler not found after Register")
	}

	raw, err := json.Marshal(addRequest{A: 3, B: 4})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	got, err := h.Handle(context.Background(), json.RawMessage(raw))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}

	var resp addResponse
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if resp.Sum != 7 {
		t.Errorf("got Sum=%d, want 7", resp.Sum)
	}
}

func TestRegister_BadJSON(t *testing.T) {
	rt := newRuntime()
	jobs.Register(rt, "add", func(ctx context.Context, req addRequest) (addResponse, error) {
		t.Fatal("handler should not be called with invalid JSON")
		return addResponse{}, nil
	})

	h, ok := rt.Lookup("add")
	if !ok {
		t.Fatal("handler not found after Register")
	}

	_, err := h.Handle(context.Background(), json.RawMessage(`not valid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON input, got nil")
	}
}

func TestRegister_HandlerError(t *testing.T) {
	want := errors.New("handler failure")
	rt := newRuntime()
	jobs.Register(rt, "add", func(ctx context.Context, req addRequest) (addResponse, error) {
		return addResponse{}, want
	})

	h, ok := rt.Lookup("add")
	if !ok {
		t.Fatal("handler not found after Register")
	}

	raw, _ := json.Marshal(addRequest{A: 1, B: 2})
	_, err := h.Handle(context.Background(), json.RawMessage(raw))
	if !errors.Is(err, want) {
		t.Errorf("got err=%v, want %v", err, want)
	}
}

func TestRegister_EmptyRequest(t *testing.T) {
	type emptyReq struct{}
	type emptyResp struct{ OK bool }

	rt := newRuntime()
	jobs.Register(rt, "noop", func(ctx context.Context, req emptyReq) (emptyResp, error) {
		return emptyResp{OK: true}, nil
	})

	h, ok := rt.Lookup("noop")
	if !ok {
		t.Fatal("handler not found after Register")
	}

	got, err := h.Handle(context.Background(), json.RawMessage(`{}`))
	if err != nil {
		t.Fatalf("Handle: %v", err)
	}
	var resp emptyResp
	if err := json.Unmarshal(got, &resp); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if !resp.OK {
		t.Error("expected OK=true")
	}
}

func TestRegister_Lookup_UnknownName(t *testing.T) {
	rt := newRuntime()
	_, ok := rt.Lookup("nonexistent")
	if ok {
		t.Error("expected Lookup to return false for unregistered handler")
	}
}

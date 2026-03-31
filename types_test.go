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

func TestJobFn_Handle_RoundTrip(t *testing.T) {
	fn := jobs.JobFn[addRequest, addResponse](func(ctx context.Context, req addRequest) (addResponse, error) {
		return addResponse{Sum: req.A + req.B}, nil
	})

	raw, err := json.Marshal(addRequest{A: 3, B: 4})
	if err != nil {
		t.Fatalf("marshal request: %v", err)
	}

	got, err := fn.Handle(context.Background(), json.RawMessage(raw))
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

func TestJobFn_Handle_BadJSON(t *testing.T) {
	fn := jobs.JobFn[addRequest, addResponse](func(ctx context.Context, req addRequest) (addResponse, error) {
		t.Fatal("handler should not be called with invalid JSON")
		return addResponse{}, nil
	})

	_, err := fn.Handle(context.Background(), json.RawMessage(`not valid json`))
	if err == nil {
		t.Fatal("expected error for invalid JSON input, got nil")
	}
}

func TestJobFn_Handle_HandlerError(t *testing.T) {
	want := errors.New("handler failure")
	fn := jobs.JobFn[addRequest, addResponse](func(ctx context.Context, req addRequest) (addResponse, error) {
		return addResponse{}, want
	})

	raw, _ := json.Marshal(addRequest{A: 1, B: 2})
	_, err := fn.Handle(context.Background(), json.RawMessage(raw))
	if !errors.Is(err, want) {
		t.Errorf("got err=%v, want %v", err, want)
	}
}

func TestJobFn_Handle_EmptyRequest(t *testing.T) {
	type emptyReq struct{}
	type emptyResp struct{ OK bool }

	fn := jobs.JobFn[emptyReq, emptyResp](func(ctx context.Context, req emptyReq) (emptyResp, error) {
		return emptyResp{OK: true}, nil
	})

	got, err := fn.Handle(context.Background(), json.RawMessage(`{}`))
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

package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"
)

// GetVtctldOperationRequest is a request for retrieving a vtctld operation.
type GetVtctldOperationRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	ID           string `json:"-"`
}

// VtctldOperationReference identifies an accepted vtctld operation that can be
// polled later.
type VtctldOperationReference struct {
	ID string `json:"id"`
}

// VtctldOperation represents a generic vtctld operation resource.
type VtctldOperation struct {
	ID          string          `json:"id"`
	Type        string          `json:"type"`
	Action      string          `json:"action"`
	Timeout     int             `json:"timeout"`
	CreatedAt   time.Time       `json:"created_at"`
	CompletedAt *time.Time      `json:"completed_at"`
	State       string          `json:"state"`
	Completed   bool            `json:"completed"`
	Metadata    json.RawMessage `json:"metadata"`
	Result      json.RawMessage `json:"result"`
	Error       string          `json:"error"`
}

func vtctldOperationsAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "vtctld", "operations")
}

func vtctldOperationAPIPath(org, db, branch, id string) string {
	return path.Join(vtctldOperationsAPIPath(org, db, branch), id)
}

func (s *vtctldService) GetOperation(ctx context.Context, req *GetVtctldOperationRequest) (*VtctldOperation, error) {
	p := vtctldOperationAPIPath(req.Organization, req.Database, req.Branch, req.ID)
	httpReq, err := s.client.newRequest(http.MethodGet, p, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	resp := &VtctldOperation{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

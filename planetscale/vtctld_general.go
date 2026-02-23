package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

// VtctldService is an interface for interacting with the general vtctld endpoints of the
// PlanetScale API.
type VtctldService interface {
	ListWorkflows(context.Context, *VtctldListWorkflowsRequest) (json.RawMessage, error)
	ListKeyspaces(context.Context, *VtctldListKeyspacesRequest) (json.RawMessage, error)
}

type VtctldListWorkflowsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
	Workflow     string `json:"-"`
}

type VtctldListKeyspacesRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Name         string `json:"-"`
}

type vtctldService struct {
	client *Client
}

var _ VtctldService = &vtctldService{}

func vtctldWorkflowsAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "vtctld", "workflows")
}

func vtctldKeyspacesAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "vtctld", "keyspaces")
}

func (s *vtctldService) ListWorkflows(ctx context.Context, req *VtctldListWorkflowsRequest) (json.RawMessage, error) {
	p := vtctldWorkflowsAPIPath(req.Organization, req.Database, req.Branch)
	v := url.Values{}
	v.Set("keyspace", req.Keyspace)
	if req.Workflow != "" {
		v.Set("workflow", req.Workflow)
	}
	httpReq, err := s.client.newRequest(http.MethodGet, p, nil, WithQueryParams(v))
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *vtctldService) ListKeyspaces(ctx context.Context, req *VtctldListKeyspacesRequest) (json.RawMessage, error) {
	p := vtctldKeyspacesAPIPath(req.Organization, req.Database, req.Branch)
	v := url.Values{}
	if req.Name != "" {
		v.Set("name", req.Name)
	}
	httpReq, err := s.client.newRequest(http.MethodGet, p, nil, WithQueryParams(v))
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

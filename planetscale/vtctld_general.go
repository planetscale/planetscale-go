package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
)

// VtctldService is an interface for interacting with the general vtctld endpoints of the
// PlanetScale API.
type VtctldService interface {
	ListWorkflows(context.Context, *VtctldListWorkflowsRequest) (json.RawMessage, error)
	ListKeyspaces(context.Context, *VtctldListKeyspacesRequest) (json.RawMessage, error)
	StartWorkflow(context.Context, *VtctldStartWorkflowRequest) (json.RawMessage, error)
	StopWorkflow(context.Context, *VtctldStopWorkflowRequest) (json.RawMessage, error)
	GetOperation(context.Context, *GetVtctldOperationRequest) (*VtctldOperation, error)
}

type VtctldListWorkflowsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
	Workflow     string `json:"-"`
	IncludeLogs  *bool  `json:"-"`
}

type VtctldListKeyspacesRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Name         string `json:"-"`
}

// VtctldStartWorkflowRequest is a request for starting a workflow.
type VtctldStartWorkflowRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Workflow     string `json:"-"`
	Keyspace     string `json:"keyspace"`
}

// VtctldStopWorkflowRequest is a request for stopping a workflow.
type VtctldStopWorkflowRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Workflow     string `json:"-"`
	Keyspace     string `json:"keyspace"`
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
	if req.IncludeLogs != nil {
		v.Set("include_logs", strconv.FormatBool(*req.IncludeLogs))
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

func vtctldWorkflowAPIPath(org, db, branch, workflow string) string {
	return path.Join(vtctldWorkflowsAPIPath(org, db, branch), workflow)
}

func (s *vtctldService) StartWorkflow(ctx context.Context, req *VtctldStartWorkflowRequest) (json.RawMessage, error) {
	p := path.Join(vtctldWorkflowAPIPath(req.Organization, req.Database, req.Branch, req.Workflow), "start")
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *vtctldService) StopWorkflow(ctx context.Context, req *VtctldStopWorkflowRequest) (json.RawMessage, error) {
	p := path.Join(vtctldWorkflowAPIPath(req.Organization, req.Database, req.Branch, req.Workflow), "stop")
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

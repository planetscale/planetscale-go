package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

// Process is a single row from `SHOW FULL PROCESSLIST` on the resolved primary
// tablet.
type Process struct {
	ID      int64  `json:"id"`
	User    string `json:"user"`
	Host    string `json:"host"`
	DB      string `json:"db"`
	Command string `json:"command"`
	Time    int64  `json:"time"`
	State   string `json:"state"`
	Info    string `json:"info"`
}

// ProcesslistResult is the response from showing the process list. It echoes the
// keyspace/shard/tablet that were resolved so the caller knows where the
// processes (and their IDs) live.
type ProcesslistResult struct {
	Keyspace  string    `json:"keyspace"`
	Shard     string    `json:"shard"`
	Tablet    string    `json:"tablet"`
	Processes []Process `json:"processes"`
}

// KillProcessResult is the response from killing a process.
type KillProcessResult struct {
	Success  bool   `json:"success"`
	Keyspace string `json:"keyspace"`
	Shard    string `json:"shard"`
	Tablet   string `json:"tablet"`
	ID       int64  `json:"id"`
	Kind     string `json:"kind"`
}

// ProcesslistRequest lists the MySQL process list for a branch. Keyspace/Shard
// are only required to disambiguate multi-keyspace or sharded databases; for a
// single unsharded keyspace they may be left empty.
type ProcesslistRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
	Shard        string `json:"-"`
}

// KillProcessRequest kills a single process by ID on a branch. Keyspace/Shard
// follow the same disambiguation rules as ProcesslistRequest. Kind is either
// "connection" (default) or "query".
type KillProcessRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"keyspace,omitempty"`
	Shard        string `json:"shard,omitempty"`
	ID           int64  `json:"id"`
	Kind         string `json:"kind,omitempty"`
}

// ProcesslistService is an interface for viewing and killing MySQL processes on
// a Vitess branch.
type ProcesslistService interface {
	List(context.Context, *ProcesslistRequest) (*ProcesslistResult, error)
	Kill(context.Context, *KillProcessRequest) (*KillProcessResult, error)
}

type processlistService struct {
	client *Client
}

var _ ProcesslistService = &processlistService{}

type processlistResponse struct {
	Data *ProcesslistResult `json:"data"`
}

type killProcessResponse struct {
	Data *KillProcessResult `json:"data"`
}

func processlistAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "processlist")
}

func (s *processlistService) List(ctx context.Context, req *ProcesslistRequest) (*ProcesslistResult, error) {
	p := processlistAPIPath(req.Organization, req.Database, req.Branch)
	v := url.Values{}
	if req.Keyspace != "" {
		v.Set("keyspace", req.Keyspace)
	}
	if req.Shard != "" {
		v.Set("shard", req.Shard)
	}

	httpReq, err := s.client.newRequest(http.MethodGet, p, nil, WithQueryParams(v))
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	resp := &processlistResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}

	return resp.Data, nil
}

func (s *processlistService) Kill(ctx context.Context, req *KillProcessRequest) (*KillProcessResult, error) {
	p := path.Join(processlistAPIPath(req.Organization, req.Database, req.Branch), "kill")

	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	resp := &killProcessResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}

	return resp.Data, nil
}

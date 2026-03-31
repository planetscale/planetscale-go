package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"path"
)

// PlannedReparentShardService is an interface for interacting with the
// PlannedReparentShard endpoints of the PlanetScale API.
type PlannedReparentShardService interface {
	Create(context.Context, *PlannedReparentShardRequest) (*VtctldOperation, error)
	Get(context.Context, *GetPlannedReparentShardRequest) (*VtctldOperation, error)
}

// PlannedReparentShardRequest is a request for creating a planned reparent
// shard operation.
type PlannedReparentShardRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"keyspace"`
	Shard        string `json:"shard"`
	NewPrimary   string `json:"new_primary"`
}

// GetPlannedReparentShardRequest is a request for retrieving a planned reparent
// shard operation.
type GetPlannedReparentShardRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	ID           string `json:"-"`
}

type plannedReparentShardService struct {
	client *Client
}

var _ PlannedReparentShardService = &plannedReparentShardService{}

func plannedReparentAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "planned-reparent")
}

func (s *plannedReparentShardService) Create(ctx context.Context, req *PlannedReparentShardRequest) (*VtctldOperation, error) {
	p := plannedReparentAPIPath(req.Organization, req.Database, req.Branch)
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	resp := &VtctldOperation{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (s *plannedReparentShardService) Get(ctx context.Context, req *GetPlannedReparentShardRequest) (*VtctldOperation, error) {
	p := path.Join(plannedReparentAPIPath(req.Organization, req.Database, req.Branch), req.ID)
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

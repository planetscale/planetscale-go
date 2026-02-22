package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"
)

// BranchInfrastructure represents the infrastructure (pods) for a branch.
type BranchInfrastructure struct {
	Type  string               `json:"type"`
	Ready bool                 `json:"ready"`
	Pods  []*BranchInfraPod    `json:"pods"`
}

// BranchInfraPod represents a single pod in the branch infrastructure.
type BranchInfraPod struct {
	Name         string     `json:"name"`
	Status       string     `json:"status"`
	Component    string     `json:"component"`
	Ready        string     `json:"ready"`
	RestartCount int        `json:"restart_count"`
	CreatedAt    *time.Time `json:"created_at"`
	Cell         string     `json:"cell"`
	Size         string     `json:"size"`
	Keyspace     *string    `json:"keyspace"`
	Shard        *string    `json:"shard"`
	TabletType   *string    `json:"tablet_type"`
}

// GetBranchInfrastructureRequest encapsulates the request for getting branch infrastructure.
type GetBranchInfrastructureRequest struct {
	Organization string
	Database     string
	Branch       string
}

// BranchInfrastructureService is an interface for interacting with the branch infrastructure API.
type BranchInfrastructureService interface {
	Get(ctx context.Context, req *GetBranchInfrastructureRequest) (*BranchInfrastructure, error)
}

type branchInfrastructureService struct {
	client *Client
}

var _ BranchInfrastructureService = &branchInfrastructureService{}

func NewBranchInfrastructureService(client *Client) *branchInfrastructureService {
	return &branchInfrastructureService{
		client: client,
	}
}

func (s *branchInfrastructureService) Get(ctx context.Context, getReq *GetBranchInfrastructureRequest) (*BranchInfrastructure, error) {
	p := path.Join(
		databaseBranchAPIPath(getReq.Organization, getReq.Database, getReq.Branch),
		"infrastructure",
	)

	req, err := s.client.newRequest(http.MethodGet, p, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for get branch infrastructure: %w", err)
	}

	infra := &BranchInfrastructure{}
	if err := s.client.do(ctx, req, &infra); err != nil {
		return nil, err
	}

	return infra, nil
}

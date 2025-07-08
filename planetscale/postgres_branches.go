package planetscale

import (
	"context"
	"fmt"
	"net/http"
)

type PostgresBranch struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	ClusterName         string `json:"cluster_name"`
	ClusterDisplayName  string `json:"cluster_display_name"`
	ClusterArchitecture string `json:"cluster_architecture"`
}

// CreatePostgresBranchRequest encapsulates the request to create a Postgres branch.
type CreatePostgresBranchRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Region       string `json:"region,omitempty"`
	Name         string `json:"name"`
	ParentBranch string `json:"parent_branch"`
	BackupID     string `json:"backup_id,omitempty"`
	ClusterName  string `json:"cluster_name,omitempty"`
}

type ListPostgresBranchesRequest struct{}

type GetPostgresBranchRequest struct{}

type DeletePostgresBranchRequest struct{}

type PostgresBranchSchemaRequest struct{}

// PostgresBranchSchema encapsulates the schema of a Postgres branch.
type PostgresBranchSchema struct {
	Name string `json:"name"`
	Raw  string `json:"raw"`
	HTML string `json:"html"`
}

type PostgresBranchesService interface {
	Create(context.Context, *CreatePostgresBranchRequest) (*PostgresBranch, error)
	List(context.Context, *ListPostgresBranchesRequest) ([]*PostgresBranch, error)
	Get(context.Context, *GetPostgresBranchRequest) (*PostgresBranch, error)
	Delete(context.Context, *DeletePostgresBranchRequest) error
	Schema(context.Context, *PostgresBranchSchemaRequest) (*PostgresBranchSchema, error)
	ListClusterSKUs(context.Context, *ListBranchClusterSKUsRequest) ([]*ClusterSKU, error)
}

type postgresBranchesService struct {
	client *Client
}

var _ PostgresBranchesService = &postgresBranchesService{}

func NewPostgresBranchesService(client *Client) *postgresBranchesService {
	return &postgresBranchesService{
		client: client,
	}
}

func (p *postgresBranchesService) Create(ctx context.Context, createReq *CreatePostgresBranchRequest) (*PostgresBranch, error) {
	path := databaseBranchesAPIPath(createReq.Organization, createReq.Database)
	req, err := p.client.newRequest(http.MethodPost, path, createReq)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	b := &PostgresBranch{}
	if err := p.client.do(ctx, req, b); err != nil {
		return nil, err
	}

	return b, nil
}

// Delete implements PostgresBranchesService.
func (p *postgresBranchesService) Delete(context.Context, *DeletePostgresBranchRequest) error {
	panic("unimplemented")
}

// Get implements PostgresBranchesService.
func (p *postgresBranchesService) Get(context.Context, *GetPostgresBranchRequest) (*PostgresBranch, error) {
	panic("unimplemented")
}

// List implements PostgresBranchesService.
func (p *postgresBranchesService) List(context.Context, *ListPostgresBranchesRequest) ([]*PostgresBranch, error) {
	panic("unimplemented")
}

// ListClusterSKUs implements PostgresBranchesService.
func (p *postgresBranchesService) ListClusterSKUs(context.Context, *ListBranchClusterSKUsRequest) ([]*ClusterSKU, error) {
	panic("unimplemented")
}

// Schema implements PostgresBranchesService.
func (p *postgresBranchesService) Schema(context.Context, *PostgresBranchSchemaRequest) (*PostgresBranchSchema, error) {
	panic("unimplemented")
}

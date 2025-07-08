package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"
)

// PostgresBranch represents a Postgres branch in the PlanetScale API.
type PostgresBranch struct {
	ID                  string    `json:"id"`
	Name                string    `json:"name"`
	ClusterName         string    `json:"cluster_name"`
	ClusterDisplayName  string    `json:"cluster_display_name"`
	ClusterArchitecture string    `json:"cluster_architecture"`
	ClusterIOPS         int       `json:"cluster_iops"`
	State               string    `json:"state"`
	CreatedAt           time.Time `json:"created_at"`
	UpdatedAt           time.Time `json:"updated_at"`
	Actor               Actor     `json:"actor"`
	Production          bool      `json:"production"`
	Ready               bool      `json:"ready"`
	ParentBranch        string    `json:"parent_branch"`
	Region              Region    `json:"region"`
	Kind                string    `json:"kind"`
}

type postgresBranchesResponse struct {
	Branches []*PostgresBranch `json:"data"`
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

// ListPostgresBranchesRequest encapsulates the request to list Postgres branches for a database.
type ListPostgresBranchesRequest struct {
	Organization string
	Database     string
}

// GetPostgresBranchRequest encapsulates the request to get a specific Postgres branch.
type GetPostgresBranchRequest struct {
	Organization string
	Database     string
	Branch       string
}

// DeletePostgresBranchRequest encapsulates the request to delete a Postgres branch.
type DeletePostgresBranchRequest struct {
	Organization string
	Database     string
	Branch       string
}

// PostgresBranchSchemaRequest encapsulates the request to get the schema of a Postgres branch.
type PostgresBranchSchemaRequest struct {
	Organization string
	Database     string
	Branch       string
	Namespace    string `json:"-"`
}

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
	ListClusterSKUs(context.Context, *ListBranchClusterSKUsRequest, ...ListOption) ([]*ClusterSKU, error)
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

// Create creates a new Postgres branch in the specified organization and database.
func (p *postgresBranchesService) Create(ctx context.Context, createReq *CreatePostgresBranchRequest) (*PostgresBranch, error) {
	path := postgresBranchesAPIPath(createReq.Organization, createReq.Database)
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

// List returns a list of Postgres branches for the specified organization and database.
func (p *postgresBranchesService) List(ctx context.Context, listReq *ListPostgresBranchesRequest) ([]*PostgresBranch, error) {
	req, err := p.client.newRequest(http.MethodGet, postgresBranchesAPIPath(listReq.Organization, listReq.Database), nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	pgBranches := &postgresBranchesResponse{}
	if err := p.client.do(ctx, req, &pgBranches); err != nil {
		return nil, err
	}

	return pgBranches.Branches, nil
}

// Get returns a single Postgres branch for the specified organization, database, and branch.
func (p *postgresBranchesService) Get(ctx context.Context, getReq *GetPostgresBranchRequest) (*PostgresBranch, error) {
	path := fmt.Sprintf("%s/%s", postgresBranchesAPIPath(getReq.Organization, getReq.Database), getReq.Branch)
	req, err := p.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	pgBranch := &PostgresBranch{}
	if err := p.client.do(ctx, req, &pgBranch); err != nil {
		return nil, err
	}

	return pgBranch, nil
}

// Delete deletes a Postgres branch from the specified organization and database.
func (p *postgresBranchesService) Delete(ctx context.Context, deleteReq *DeletePostgresBranchRequest) error {
	path := fmt.Sprintf("%s/%s", postgresBranchesAPIPath(deleteReq.Organization, deleteReq.Database), deleteReq.Branch)
	req, err := p.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return fmt.Errorf("error creating http request: %w", err)
	}

	err = p.client.do(ctx, req, nil)
	return err
}

// ListClusterSKUs returns a list of cluster SKUs for the specified Postgres branch.
func (p *postgresBranchesService) ListClusterSKUs(ctx context.Context, listReq *ListBranchClusterSKUsRequest, opts ...ListOption) ([]*ClusterSKU, error) {
	path := fmt.Sprintf("%s/cluster-size-skus", postgresBranchAPIPath(listReq.Organization, listReq.Database, listReq.Branch))

	defaultOpts := defaultListOptions()
	for _, opt := range opts {
		err := opt(defaultOpts)
		if err != nil {
			return nil, err
		}
	}

	if vals := defaultOpts.URLValues.Encode(); vals != "" {
		path += "?" + vals
	}

	req, err := p.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	clusterSKUs := []*ClusterSKU{}
	if err := p.client.do(ctx, req, &clusterSKUs); err != nil {
		return nil, err
	}

	return clusterSKUs, nil
}

// Schema returns the schema for the specified Postgres branch.
func (p *postgresBranchesService) Schema(ctx context.Context, schemaReq *PostgresBranchSchemaRequest) (*PostgresBranchSchema, error) {
	path := fmt.Sprintf("%s/schema", postgresBranchAPIPath(schemaReq.Organization, schemaReq.Database, schemaReq.Branch))
	v := url.Values{}
	if schemaReq.Namespace != "" {
		v.Set("namespace", schemaReq.Namespace)
	}

	if vals := v.Encode(); vals != "" {
		path += "?" + vals
	}

	req, err := p.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	schema := &PostgresBranchSchema{}
	if err := p.client.do(ctx, req, &schema); err != nil {
		return nil, err
	}

	return schema, nil
}

func postgresBranchesAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/branches", databasesAPIPath(org), db)
}

func postgresBranchAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/%s", postgresBranchesAPIPath(org, db), branch)
}

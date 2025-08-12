package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"path"
	"time"
)

// PostgresRole represents a PostgreSQL role in PlanetScale.
type PostgresRole struct {
	ID            string    `json:"id"`
	Name          string    `json:"name"`
	AccessHostURL string    `json:"access_host_url"`
	DatabaseName  string    `json:"database_name"`
	Password      string    `json:"password"`
	Actor         Actor     `json:"actor"`
	Username      string    `json:"username"`
	CreatedAt     time.Time `json:"created_at"`
}

type postgresRolesResponse struct {
	Roles []*PostgresRole `json:"data"`
}

// ListPostgresRolesRequest encapsulates the request for listing all roles for a given database branch.
type ListPostgresRolesRequest struct {
	Organization string
	Database     string
	Branch       string
}

// GetPostgresRoleRequest encapsulates the request for getting a specific role for a given database branch.
type GetPostgresRoleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	RoleId       string
}

// CreatePostgresRoleRequest encapsulates the request for creating role credentials for a database branch.
type CreatePostgresRoleRequest struct {
	Organization   string   `json:"-"`
	Database       string   `json:"-"`
	Branch         string   `json:"-"`
	Name           string   `json:"name"`
	TTL            int      `json:"ttl,omitempty"`
	InheritedRoles []string `json:"inherited_roles,omitempty"`
}

// UpdatePostgresRoleRequest encapsulates the request for updating a role name for a database branch.
type UpdatePostgresRoleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	RoleId       string `json:"-"`
	Name         string `json:"name"`
}

// RenewPostgresRoleRequest encapsulates the request for renewing role expiration for a database branch.
type RenewPostgresRoleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	RoleId       string `json:"-"`
}

// DeletePostgresRoleRequest encapsulates the request for deleting role credentials for a database branch.
type DeletePostgresRoleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	RoleId       string `json:"-"`
	Successor    string `json:"successor,omitempty"`
}

// ResetDefaultRoleRequest encapsulates the request for resetting the default role of a Postgres database branch.
type ResetDefaultRoleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// PostgresRolesService defines the interface for managing PostgreSQL roles in PlanetScale.
type PostgresRolesService interface {
	List(context.Context, *ListPostgresRolesRequest, ...ListOption) ([]*PostgresRole, error)
	Get(context.Context, *GetPostgresRoleRequest) (*PostgresRole, error)
	Create(context.Context, *CreatePostgresRoleRequest) (*PostgresRole, error)
	Update(context.Context, *UpdatePostgresRoleRequest) (*PostgresRole, error)
	Renew(context.Context, *RenewPostgresRoleRequest) (*PostgresRole, error)
	Delete(context.Context, *DeletePostgresRoleRequest) error
	ResetDefaultRole(context.Context, *ResetDefaultRoleRequest) (*PostgresRole, error)
}

type postgresRolesService struct {
	client *Client
}

var _ PostgresRolesService = &postgresRolesService{}

func NewPostgresRolesService(client *Client) *postgresRolesService {
	return &postgresRolesService{
		client: client,
	}
}

// ResetDefaultRole resets the default role for a PostgreSQL database branch.
func (p *postgresRolesService) ResetDefaultRole(ctx context.Context, resetReq *ResetDefaultRoleRequest) (*PostgresRole, error) {
	pathStr := path.Join(postgresBranchRolesAPIPath(resetReq.Organization, resetReq.Database, resetReq.Branch), "reset-default")
	req, err := p.client.newRequest(http.MethodPost, pathStr, resetReq)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	role := &PostgresRole{}
	if err := p.client.do(ctx, req, &role); err != nil {
		return nil, err
	}

	return role, nil
}

// List all existing roles for a database branch.
func (p *postgresRolesService) List(ctx context.Context, listReq *ListPostgresRolesRequest, opts ...ListOption) ([]*PostgresRole, error) {
	pathStr := postgresBranchRolesAPIPath(listReq.Organization, listReq.Database, listReq.Branch)

	defaultOpts := defaultListOptions(WithPerPage(50))
	for _, opt := range opts {
		err := opt(defaultOpts)
		if err != nil {
			return nil, err
		}
	}

	req, err := p.client.newRequest(http.MethodGet, pathStr, nil, WithQueryParams(*defaultOpts.URLValues))
	if err != nil {
		return nil, fmt.Errorf("error creating http request to list roles: %w", err)
	}

	rolesResp := &postgresRolesResponse{}
	if err := p.client.do(ctx, req, &rolesResp); err != nil {
		return nil, err
	}

	return rolesResp.Roles, nil
}

// Get an existing role for a database branch.
func (p *postgresRolesService) Get(ctx context.Context, getReq *GetPostgresRoleRequest) (*PostgresRole, error) {
	pathStr := postgresBranchRoleAPIPath(getReq.Organization, getReq.Database, getReq.Branch, getReq.RoleId)
	req, err := p.client.newRequest(http.MethodGet, pathStr, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	role := &PostgresRole{}
	if err := p.client.do(ctx, req, &role); err != nil {
		return nil, err
	}

	return role, nil
}

// Create role credentials for a database branch.
func (p *postgresRolesService) Create(ctx context.Context, createReq *CreatePostgresRoleRequest) (*PostgresRole, error) {
	pathStr := postgresBranchRolesAPIPath(createReq.Organization, createReq.Database, createReq.Branch)
	req, err := p.client.newRequest(http.MethodPost, pathStr, createReq)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	role := &PostgresRole{}
	if err := p.client.do(ctx, req, &role); err != nil {
		return nil, err
	}

	return role, nil
}

// Update role name for a database branch.
func (p *postgresRolesService) Update(ctx context.Context, updateReq *UpdatePostgresRoleRequest) (*PostgresRole, error) {
	pathStr := postgresBranchRoleAPIPath(updateReq.Organization, updateReq.Database, updateReq.Branch, updateReq.RoleId)
	req, err := p.client.newRequest(http.MethodPatch, pathStr, updateReq)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	role := &PostgresRole{}
	if err := p.client.do(ctx, req, &role); err != nil {
		return nil, err
	}

	return role, nil
}

// Renew role expiration for a database branch.
func (p *postgresRolesService) Renew(ctx context.Context, renewReq *RenewPostgresRoleRequest) (*PostgresRole, error) {
	pathStr := postgresBranchRoleRenewAPIPath(renewReq.Organization, renewReq.Database, renewReq.Branch, renewReq.RoleId)
	req, err := p.client.newRequest(http.MethodPost, pathStr, renewReq)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	role := &PostgresRole{}
	if err := p.client.do(ctx, req, &role); err != nil {
		return nil, err
	}

	return role, nil
}

// Delete role credentials for a database branch.
func (p *postgresRolesService) Delete(ctx context.Context, deleteReq *DeletePostgresRoleRequest) error {
	pathStr := postgresBranchRoleAPIPath(deleteReq.Organization, deleteReq.Database, deleteReq.Branch, deleteReq.RoleId)
	req, err := p.client.newRequest(http.MethodDelete, pathStr, deleteReq)
	if err != nil {
		return fmt.Errorf("error creating http request: %w", err)
	}

	err = p.client.do(ctx, req, nil)
	return err
}

func postgresBranchRoleRenewAPIPath(org, db, branch, roleId string) string {
	return path.Join(postgresBranchRoleAPIPath(org, db, branch, roleId), "renew")
}

func postgresBranchRoleAPIPath(org, db, branch, roleId string) string {
	return path.Join(postgresBranchRolesAPIPath(org, db, branch), roleId)
}

func postgresBranchRolesAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "roles")
}

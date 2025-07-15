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

// ResetDefaultRoleRequest encapsulates the request for resetting the default role of a Postgres database branch.
type ResetDefaultRoleRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// PostgresRolesService defines the interface for managing PostgreSQL roles in PlanetScale.
type PostgresRolesService interface {
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

func postgresBranchRolesAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "roles")
}

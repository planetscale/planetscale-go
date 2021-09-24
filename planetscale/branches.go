package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// DatabaseBranch represents a database branch.
type DatabaseBranch struct {
	Name          string    `json:"name"`
	ParentBranch  string    `json:"parent_branch"`
	Region        Region    `json:"region"`
	Ready         bool      `json:"ready"`
	Production    bool      `json:"production"`
	CreatedAt     time.Time `json:"created_at"`
	UpdatedAt     time.Time `json:"updated_at"`
	AccessHostURL string    `json:"access_host_url"`
}

type databaseBranchesResponse struct {
	Branches []*DatabaseBranch `json:"data"`
}

// CreateDatabaseBranchRequest encapsulates the request for creating a new
// database branch
type CreateDatabaseBranchRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Region       string `json:"region,omitempty"`
	Name         string `json:"name"`
	ParentBranch string `json:"parent_branch"`
	BackupID     string `json:"backup_id,omitempty"`
}

// ListDatabaseBranchesRequest encapsulates the request for listing the branches
// of a database.
type ListDatabaseBranchesRequest struct {
	Organization string
	Database     string
}

// GetDatabaseBranchRequest encapsulates the request for getting a single
// database branch for a database.
type GetDatabaseBranchRequest struct {
	Organization string
	Database     string
	Branch       string
}

// DeleteDatabaseRequest encapsulates the request for deleting a database branch
// from a database.
type DeleteDatabaseBranchRequest struct {
	Organization string
	Database     string
	Branch       string
}

// DiffBranchRequest encapsulates a request for getting the diff for a branch.
type DiffBranchRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// BranchSchemaRequest encapsulates a request for getting a branch's schema.
type BranchSchemaRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// RefreshSchemaRequest reflects the request needed to refresh a schema
// snapshot on a database branch.
type RefreshSchemaRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// PromoteRequest encapsulates the request for promoting a branch to
// production.
type PromoteRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

type GetPromotionRequestRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

type PromotionRequestError struct {
	Message string `json:"message"`
	DocsUrl string `json:"docs_url"`
}

// BranchPromotionRequest represents a promotion request for a branch.
type BranchPromotionRequest struct {
	ID                    string                 `json:"id"`
	Branch                string                 `json:"branch"`
	PromotionRequestError *PromotionRequestError `json:"promotion_request_error"`
	State                 string                 `json:"state"`
	CreatedAt             time.Time              `json:"created_at"`
	UpdatedAt             time.Time              `json:"updated_at"`
	StartedAt             *time.Time             `json:"started_at"`
	FinishedAt            *time.Time             `json:"finished_at"`
}

// DatabaseBranchesService is an interface for communicating with the PlanetScale
// Database Branch API endpoint.
type DatabaseBranchesService interface {
	Create(context.Context, *CreateDatabaseBranchRequest) (*DatabaseBranch, error)
	List(context.Context, *ListDatabaseBranchesRequest) ([]*DatabaseBranch, error)
	Get(context.Context, *GetDatabaseBranchRequest) (*DatabaseBranch, error)
	Delete(context.Context, *DeleteDatabaseBranchRequest) error
	Diff(context.Context, *DiffBranchRequest) ([]*Diff, error)
	Schema(context.Context, *BranchSchemaRequest) ([]*Diff, error)
	RefreshSchema(context.Context, *RefreshSchemaRequest) error
	Promote(context.Context, *PromoteRequest) (*BranchPromotionRequest, error)
	GetPromotionRequest(context.Context, *GetPromotionRequestRequest) (*BranchPromotionRequest, error)
}

type databaseBranchesService struct {
	client *Client
}

var _ DatabaseBranchesService = &databaseBranchesService{}

func NewDatabaseBranchesService(client *Client) *databaseBranchesService {
	return &databaseBranchesService{
		client: client,
	}
}

func (d *databaseBranchesService) Diff(ctx context.Context, diffReq *DiffBranchRequest) ([]*Diff, error) {
	path := fmt.Sprintf("%s/diff", databaseBranchAPIPath(diffReq.Organization, diffReq.Database, diffReq.Branch))
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	diffs := &diffResponse{}
	if err := d.client.do(ctx, req, &diffs); err != nil {
		return nil, err
	}

	return diffs.Diffs, nil
}

// schemaResponse returns the schemas
type schemaResponse struct {
	Schemas []*Diff `json:"data"`
}

func (d *databaseBranchesService) Schema(ctx context.Context, schemaReq *BranchSchemaRequest) ([]*Diff, error) {
	path := fmt.Sprintf("%s/schema", databaseBranchAPIPath(schemaReq.Organization, schemaReq.Database, schemaReq.Branch))
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	schemas := &schemaResponse{}
	if err := d.client.do(ctx, req, &schemas); err != nil {
		return nil, err
	}

	return schemas.Schemas, nil
}

// Create creates a new branch for an organization's database.
func (d *databaseBranchesService) Create(ctx context.Context, createReq *CreateDatabaseBranchRequest) (*DatabaseBranch, error) {
	path := databaseBranchesAPIPath(createReq.Organization, createReq.Database)

	req, err := d.client.newRequest(http.MethodPost, path, createReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for branch database")
	}

	dbBranch := &DatabaseBranch{}
	if err := d.client.do(ctx, req, &dbBranch); err != nil {
		return nil, err
	}

	return dbBranch, nil
}

// Get returns a database branch for an organization's database.
func (d *databaseBranchesService) Get(ctx context.Context, getReq *GetDatabaseBranchRequest) (*DatabaseBranch, error) {
	path := fmt.Sprintf("%s/%s", databaseBranchesAPIPath(getReq.Organization, getReq.Database), getReq.Branch)
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	dbBranch := &DatabaseBranch{}
	if err := d.client.do(ctx, req, &dbBranch); err != nil {
		return nil, err
	}

	return dbBranch, nil
}

// List returns all of the branches for an organization's
// database.
func (d *databaseBranchesService) List(ctx context.Context, listReq *ListDatabaseBranchesRequest) ([]*DatabaseBranch, error) {
	req, err := d.client.newRequest(http.MethodGet, databaseBranchesAPIPath(listReq.Organization, listReq.Database), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	dbBranches := &databaseBranchesResponse{}
	if err := d.client.do(ctx, req, &dbBranches); err != nil {
		return nil, err
	}

	return dbBranches.Branches, nil
}

// Delete deletes a database branch from an organization's database.
func (d *databaseBranchesService) Delete(ctx context.Context, deleteReq *DeleteDatabaseBranchRequest) error {
	path := fmt.Sprintf("%s/%s", databaseBranchesAPIPath(deleteReq.Organization, deleteReq.Database), deleteReq.Branch)
	req, err := d.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return errors.Wrap(err, "error creating request for delete branch")
	}

	err = d.client.do(ctx, req, nil)
	return err
}

// RefreshSchema refreshes the schema for a
func (d *databaseBranchesService) RefreshSchema(ctx context.Context, refreshReq *RefreshSchemaRequest) error {
	path := fmt.Sprintf("%s/%s/refresh-schema", databaseBranchesAPIPath(refreshReq.Organization, refreshReq.Database), refreshReq.Branch)
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return errors.Wrap(err, "error creating http request")
	}

	if err := d.client.do(ctx, req, nil); err != nil {
		return err
	}

	return nil
}

// PromoteBranch promotes a database's branch from a development branch to a
// production branch.
func (d *databaseBranchesService) Promote(ctx context.Context, promoteReq *PromoteRequest) (*BranchPromotionRequest, error) {
	path := fmt.Sprintf("%s/promotion-request", databaseBranchAPIPath(promoteReq.Organization, promoteReq.Database, promoteReq.Branch))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for branch promotion")
	}

	promotionReq := &BranchPromotionRequest{}
	err = d.client.do(ctx, req, &promotionReq)
	if err != nil {
		return nil, err
	}

	return promotionReq, nil
}

func (d *databaseBranchesService) GetPromotionRequest(ctx context.Context, getReg *GetPromotionRequestRequest) (*BranchPromotionRequest, error) {
	path := fmt.Sprintf("%s/promotion-request", databaseBranchAPIPath(getReg.Organization, getReg.Database, getReg.Branch))
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for getting branch promotion request")
	}

	promotionReq := &BranchPromotionRequest{}
	err = d.client.do(ctx, req, &promotionReq)
	if err != nil {
		return nil, err
	}

	return promotionReq, nil
}

func databaseBranchesAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/branches", databasesAPIPath(org), db)
}

func databaseBranchAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/%s", databaseBranchesAPIPath(org, db), branch)
}

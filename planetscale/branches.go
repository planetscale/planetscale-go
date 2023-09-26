package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/pkg/errors"
)

// Actor represents a user or service token
type Actor struct {
	Type string `json:"type"`
	ID   string `json:"id"`
	Name string `json:"display_name"`
}

// DatabaseBranch represents a database branch.
type DatabaseBranch struct {
	Name           string    `json:"name"`
	ParentBranch   string    `json:"parent_branch"`
	Actor          Actor     `json:"actor"`
	Region         Region    `json:"region"`
	Ready          bool      `json:"ready"`
	Production     bool      `json:"production"`
	HtmlURL        string    `json:"html_url"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	AccessHostURL  string    `json:"access_host_url"`
	SafeMigrations bool      `json:"safe_migrations"`
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
	SeedData     string `json:"seed_data,omitempty"`
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
	Keyspace     string `json:"-"`
}

// BranchVSchemaRequest encapsulates a request for getting a branch's VSchema.
type BranchVSchemaRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
}

type BranchKeyspacesRequest struct {
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

// DemoteRequest encapsulates the request for demoting a branch to
// development.
type DemoteRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// PromoteRequest encapsulates the request for promoting a request to
// production.
type PromoteRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// EnableSafeMigrationsRequest encapsulates the request for enabling safe
// migrations on a branch.
type EnableSafeMigrationsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// DisableSafeMigrationsRequest encapsulates the request for disabling safe
// migrations on a branch.
type DisableSafeMigrationsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// LintSchemaRequest encapsulates the request for linting a branch's schema.
type LintSchemaRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// SchemaLintError represents an error with the branch's schema
type SchemaLintError struct {
	LintError        string `json:"lint_error"`
	Keyspace         string `json:"keyspace_name"`
	Table            string `json:"table_name"`
	SubjectType      string `json:"subject_type"`
	ErrorDescription string `json:"error_description"`
	DocsURL          string `json:"docs_url"`
}

// VSchemaDiff returns the diff for a vschema
type VSchemaDiff struct {
	Raw  string `json:"raw"`
	HTML string `json:"html"`
}

type Keyspace struct {
	ID        string    `json:"id"`
	Name      string    `json:"name"`
	Shards    int       `json:"shards"`
	Sharded   bool      `json:"sharded"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
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
	VSchema(context.Context, *BranchVSchemaRequest) (*VSchemaDiff, error)
	Keyspaces(context.Context, *BranchKeyspacesRequest) ([]*Keyspace, error)
	RefreshSchema(context.Context, *RefreshSchemaRequest) error
	Demote(context.Context, *DemoteRequest) (*DatabaseBranch, error)
	Promote(context.Context, *PromoteRequest) (*DatabaseBranch, error)
	EnableSafeMigrations(context.Context, *EnableSafeMigrationsRequest) (*DatabaseBranch, error)
	DisableSafeMigrations(context.Context, *DisableSafeMigrationsRequest) (*DatabaseBranch, error)
	LintSchema(context.Context, *LintSchemaRequest) ([]*SchemaLintError, error)
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
	v := url.Values{}
	if schemaReq.Keyspace != "" {
		v.Add("keyspace", schemaReq.Keyspace)
	}

	if vals := v.Encode(); vals != "" {
		path += "?" + vals
	}

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

func (d *databaseBranchesService) VSchema(ctx context.Context, vSchemaReq *BranchVSchemaRequest) (*VSchemaDiff, error) {
	path := fmt.Sprintf("%s/vschema", databaseBranchAPIPath(vSchemaReq.Organization, vSchemaReq.Database, vSchemaReq.Branch))
	v := url.Values{}
	if vSchemaReq.Keyspace != "" {
		v.Add("keyspace", vSchemaReq.Keyspace)
	}

	if vals := v.Encode(); vals != "" {
		path += "?" + vals
	}

	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	vSchema := &VSchemaDiff{}
	if err := d.client.do(ctx, req, &vSchema); err != nil {
		return nil, err
	}

	return vSchema, nil
}

func (d *databaseBranchesService) Keyspaces(ctx context.Context, keyspaceReq *BranchKeyspacesRequest) ([]*Keyspace, error) {
	path := fmt.Sprintf("%s/keyspaces", databaseBranchAPIPath(keyspaceReq.Organization, keyspaceReq.Database, keyspaceReq.Branch))

	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	var out struct {
		KS []*Keyspace `json:"data"`
	}

	if err := d.client.do(ctx, req, &out); err != nil {
		return nil, err
	}

	return out.KS, nil
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

// Promote promotes a branch from development to production.
func (d *databaseBranchesService) Promote(ctx context.Context, promoteReq *PromoteRequest) (*DatabaseBranch, error) {
	path := fmt.Sprintf("%s/promote", databaseBranchAPIPath(promoteReq.Organization, promoteReq.Database, promoteReq.Branch))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for branch promotion")
	}

	branch := &DatabaseBranch{}
	err = d.client.do(ctx, req, &branch)
	if err != nil {
		return nil, err
	}

	return branch, nil
}

// EnableSafeMigrations enables safe migrations for a production branch. This
// will prevent DDL statements from being performed on the branch.
func (d *databaseBranchesService) EnableSafeMigrations(ctx context.Context, enableReq *EnableSafeMigrationsRequest) (*DatabaseBranch, error) {
	path := fmt.Sprintf("%s/safe-migrations", databaseBranchAPIPath(enableReq.Organization, enableReq.Database, enableReq.Branch))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for enabling safe migrations")
	}

	branch := &DatabaseBranch{}
	err = d.client.do(ctx, req, &branch)
	if err != nil {
		return nil, err
	}

	return branch, nil
}

// DisableSafeMigrations disables safe migrations for a production branch. This
// will allow DDL statements to be performed on the branch.
func (d *databaseBranchesService) DisableSafeMigrations(ctx context.Context, disableReq *DisableSafeMigrationsRequest) (*DatabaseBranch, error) {
	path := fmt.Sprintf("%s/safe-migrations", databaseBranchAPIPath(disableReq.Organization, disableReq.Database, disableReq.Branch))
	req, err := d.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for disabling safe migrations")
	}

	branch := &DatabaseBranch{}
	err = d.client.do(ctx, req, &branch)
	if err != nil {
		return nil, err
	}

	return branch, nil
}

// Demote demotes a branch from production to development. If the branch belongs
// to an Enterprise organization, it will return a demote request and require a
// second call by a different admin in order to complete demotion.
func (d *databaseBranchesService) Demote(ctx context.Context, demoteReq *DemoteRequest) (*DatabaseBranch, error) {
	path := fmt.Sprintf("%s/demote", databaseBranchAPIPath(demoteReq.Organization, demoteReq.Database, demoteReq.Branch))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for branch demotion")
	}

	branch := &DatabaseBranch{}
	err = d.client.do(ctx, req, &branch)
	if err != nil {
		return nil, err
	}

	return branch, nil
}

// lintSchemaResponse represents the response from the lint schema endpoint.
type lintSchemaResponse struct {
	Errors []*SchemaLintError `json:"data"`
}

// LintSchema lints the current schema of a branch and returns any errors that
// may be present.
func (d *databaseBranchesService) LintSchema(ctx context.Context, lintReq *LintSchemaRequest) ([]*SchemaLintError, error) {
	path := fmt.Sprintf("%s/schema/lint", databaseBranchAPIPath(lintReq.Organization, lintReq.Database, lintReq.Branch))
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for linting branch schema")
	}

	lintResp := &lintSchemaResponse{}
	err = d.client.do(ctx, req, &lintResp)
	if err != nil {
		return nil, err
	}

	return lintResp.Errors, nil
}

func databaseBranchesAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/branches", databasesAPIPath(org), db)
}

func databaseBranchAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/%s", databaseBranchesAPIPath(org, db), branch)
}

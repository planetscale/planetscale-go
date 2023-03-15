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
	Name          string    `json:"name"`
	ParentBranch  string    `json:"parent_branch"`
	Region        Region    `json:"region"`
	Ready         bool      `json:"ready"`
	Production    bool      `json:"production"`
	HtmlURL       string    `json:"html_url"`
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

// BranchDemotionRequest represents a demotion request for a branch. This will
// only be applicable to enterprise databases.
type BranchDemotionRequest struct {
	ID          string     `json:"id"`
	Actor       *Actor     `json:"actor"`
	Responder   *Actor     `json:"responder"`
	RespondedAt *time.Time `json:"responded_at"`
	State       string     `json:"state"`
	CreatedAt   time.Time  `json:"created_at"`
	UpdatedAt   time.Time  `json:"updated_at"`
}

// DemoteRequest encapsulates the request for demoting a branch to
// development.
type DemoteRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// GetDemotionRequestRequest encapsulates the request for getting a demotion
// request for an enterprise branch.
type GetDemotionRequestRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// DenyDemotionRequest encapsulates the request for denying a demotion request
// for a branch or cancelling it (if called by the same actor who requested it).
type DenyDemotionRequestRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// RequestPromotionRequest encapsulates the request for promoting a branch to
// production.
type RequestPromotionRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

type GetPromotionRequestRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

// PromotionRequestLintError represents an error that occurs during branch
// promotion.
type PromotionRequestLintError struct {
	LintError        string `json:"lint_error"`
	Keyspace         string `json:"keyspace_name"`
	Table            string `json:"table_name"`
	SubjectType      string `json:"subject_type"`
	ErrorDescription string `json:"error_description"`
	DocsUrl          string `json:"docs_url"`
}

// BranchPromotionRequest represents a promotion request for a branch.
type BranchPromotionRequest struct {
	ID         string                       `json:"id"`
	Branch     string                       `json:"branch"`
	LintErrors []*PromotionRequestLintError `json:"lint_errors"`
	State      string                       `json:"state"`
	CreatedAt  time.Time                    `json:"created_at"`
	UpdatedAt  time.Time                    `json:"updated_at"`
	StartedAt  *time.Time                   `json:"started_at"`
	FinishedAt *time.Time                   `json:"finished_at"`
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
	Demote(context.Context, *DemoteRequest) (*BranchDemotionRequest, error)
	GetDemotionRequest(context.Context, *GetDemotionRequestRequest) (*BranchDemotionRequest, error)
	RequestPromotion(context.Context, *RequestPromotionRequest) (*BranchPromotionRequest, error)
	GetPromotionRequest(context.Context, *GetPromotionRequestRequest) (*BranchPromotionRequest, error)
	DenyDemotionRequest(context.Context, *DenyDemotionRequestRequest) error
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

// RequestPromotion requests a branch to be promoted from development to
// production.
func (d *databaseBranchesService) RequestPromotion(ctx context.Context, promoteReq *RequestPromotionRequest) (*BranchPromotionRequest, error) {
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

// GetDemotionRequest returns any pending demotion request for a given branch.
func (d *databaseBranchesService) GetDemotionRequest(ctx context.Context, getReq *GetDemotionRequestRequest) (*BranchDemotionRequest, error) {
	path := fmt.Sprintf("%s/demotion-request", databaseBranchAPIPath(getReq.Organization, getReq.Database, getReq.Branch))
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for getting branch demotion request")
	}

	demotionReq := &BranchDemotionRequest{}
	err = d.client.do(ctx, req, &demotionReq)
	if err != nil {
		return nil, err
	}

	return demotionReq, nil
}

// DenyDemotionRequest denies a pending demotion request for a given branch or
// cancels it if called by the same admin that created it.
func (d *databaseBranchesService) DenyDemotionRequest(ctx context.Context, denyReq *DenyDemotionRequestRequest) error {
	path := fmt.Sprintf("%s/demotion-request", databaseBranchAPIPath(denyReq.Organization, denyReq.Database, denyReq.Branch))
	req, err := d.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return errors.Wrap(err, "error creating request for deleting branch demotion request")
	}

	err = d.client.do(ctx, req, nil)
	return err
}

// Demote demotes a branch from production to development. If the branch belongs
// to an Enterprise organization, it will return a demote request and require a
// second call by a different admin in order to complete demotion.
func (d *databaseBranchesService) Demote(ctx context.Context, demoteReq *DemoteRequest) (*BranchDemotionRequest, error) {
	path := fmt.Sprintf("%s/demote", databaseBranchAPIPath(demoteReq.Organization, demoteReq.Database, demoteReq.Branch))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for branch demotion")
	}

	demotionReq := BranchDemotionRequest{}
	err = d.client.do(ctx, req, &demotionReq)
	if err != nil {
		return nil, err
	}

	// This is smelly but if the demotionReq does not equal the blank struct, that
	// means that something was marshaled into it, meaning a demotion request was
	// returned.
	if (demotionReq != BranchDemotionRequest{}) {
		return &demotionReq, nil
	}

	return nil, nil
}

func databaseBranchesAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/branches", databasesAPIPath(org), db)
}

func databaseBranchAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/%s", databaseBranchesAPIPath(org, db), branch)
}

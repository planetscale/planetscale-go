package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type DatabaseBranchPassword struct {
	Name      string         `json:"name"`
	Role      string         `json:"role"`
	Branch    DatabaseBranch `json:"database_branch"`
	PlainText string         `json:"plain_text"`
	CreatedAt time.Time      `json:"created_at"`
	DeletedAt time.Time      `json:"deleted_at"`
}

// DatabaseBranchPasswordRequest encapsulates the request for creating/getting/deleting a
// database branch password.
type DatabaseBranchPasswordRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Region       string `json:"-"`
	Branch       string `json:"-"`
	DisplayName  string `json:"display_name"`
}

// ListDatabaseBranchPasswordRequest encapsulates the request for listing all passwords
// for a given database branch.
type ListDatabaseBranchPasswordRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Region       string `json:"-"`
	Branch       string `json:"-"`
}

// DatabaseBranchPasswordsService is an interface for communicating with the PlanetScale
// Database Branch Passwords API endpoint.
type DatabaseBranchPasswordsService interface {
	Create(context.Context, *DatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error)
	List(context.Context, *ListDatabaseBranchPasswordRequest) ([]*DatabaseBranchPassword, error)
	Get(context.Context, *DatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error)
	Delete(context.Context, *DatabaseBranchPasswordRequest) error
}

type passwordsService struct {
	client *Client
}

type passwordsResponse struct {
	Passwords []*DatabaseBranchPassword `json:"data"`
}

var _ DatabaseBranchPasswordsService = &passwordsService{}

func NewPasswordsService(client *Client) *passwordsService {
	return &passwordsService{
		client: client,
	}
}

// Creates a new password for a branch.
func (d *passwordsService) Create(ctx context.Context, createReq *DatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error) {
	path := passwordsAPIPath(createReq.Organization, createReq.Database, createReq.Branch)
	req, err := d.client.newRequest(http.MethodPost, path, createReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	password := &DatabaseBranchPassword{}
	if err := d.client.do(ctx, req, &password); err != nil {
		return nil, err
	}

	return password, nil
}

// Delete an existing password for a branch.
func (d *passwordsService) Delete(ctx context.Context, deleteReq *DatabaseBranchPasswordRequest) error {
	path := passwordAPIPath(deleteReq.Organization, deleteReq.Database, deleteReq.Branch, deleteReq.DisplayName)
	req, err := d.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return errors.Wrap(err, "error creating http request")
	}

	err = d.client.do(ctx, req, nil)
	return err

}

// Get an existing password for a branch.
func (d *passwordsService) Get(ctx context.Context, getReq *DatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error) {
	path := passwordAPIPath(getReq.Organization, getReq.Database, getReq.Branch, getReq.DisplayName)
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	password := &DatabaseBranchPassword{}
	if err := d.client.do(ctx, req, &password); err != nil {
		return nil, err
	}

	return password, nil

}

// List all existing passwords for a branch.
func (d *passwordsService) List(ctx context.Context, listReq *ListDatabaseBranchPasswordRequest) ([]*DatabaseBranchPassword, error) {
	req, err := d.client.newRequest(http.MethodGet, passwordsAPIPath(listReq.Organization, listReq.Database, listReq.Branch), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request to list passwords")
	}

	passwordsResp := &passwordsResponse{}
	if err := d.client.do(ctx, req, &passwordsResp); err != nil {
		return nil, err
	}

	return passwordsResp.Passwords, nil
}

func passwordAPIPath(org, db, branch, password string) string {
	return fmt.Sprintf("%s/%s", passwordsAPIPath(org, db, branch), password)
}

func passwordsAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/passwords", databaseBranchAPIPath(org, db, branch))
}

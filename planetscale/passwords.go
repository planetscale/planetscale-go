package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type ConnectionStrings struct {
	DotNet   string `json:"dotnet"`
	General  string `json:"general"`
	MySQLCLI string `json:"mysqlcli"`
	PHP      string `json:"php"`
	Prisma   string `json:"prisma"`
	Rails    string `json:"rails"`
	Go       string `json:"go"`
	Java     string `json:"java"`
	Rust     string `json:"rust"`
}

type DatabaseBranchPassword struct {
	PublicID          string            `json:"id"`
	Name              string            `json:"display_name"`
	Role              string            `json:"role"`
	Branch            DatabaseBranch    `json:"database_branch"`
	CreatedAt         time.Time         `json:"created_at"`
	DeletedAt         time.Time         `json:"deleted_at"`
	PlainText         string            `json:"plain_text"`
	ConnectionStrings ConnectionStrings `json:"connection_strings"`
}

// DatabaseBranchPasswordRequest encapsulates the request for creating/getting/deleting a
// database branch password.
type DatabaseBranchPasswordRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	DisplayName  string `json:"display_name"`
}

// ListDatabaseBranchPasswordRequest encapsulates the request for listing all passwords
// for a given database branch.
type ListDatabaseBranchPasswordRequest struct {
	Organization string
	Database     string
	Branch       string
}

// GetDatabaseBranchPasswordRequest encapsulates the request for listing all passwords
// for a given database branch.
type GetDatabaseBranchPasswordRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	DisplayName  string `json:"display_name"`
	PasswordId   string
}

// DeleteDatabaseBranchPasswordRequest encapsulates the request for deleting a password
// for a given database branch.
type DeleteDatabaseBranchPasswordRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	DisplayName  string `json:"display_name"`
	PasswordId   string
}

// DatabaseBranchPasswordsService is an interface for communicating with the PlanetScale
// Database Branch Passwords API endpoint.
type PasswordsService interface {
	Create(context.Context, *DatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error)
	List(context.Context, *ListDatabaseBranchPasswordRequest) ([]*DatabaseBranchPassword, error)
	Get(context.Context, *GetDatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error)
	Delete(context.Context, *DeleteDatabaseBranchPasswordRequest) error
}

type passwordsService struct {
	client *Client
}

type passwordsResponse struct {
	Passwords []*DatabaseBranchPassword `json:"data"`
}

var _ PasswordsService = &passwordsService{}

func NewPasswordsService(client *Client) *passwordsService {
	return &passwordsService{
		client: client,
	}
}

// Creates a new password for a branch.
func (d *passwordsService) Create(ctx context.Context, createReq *DatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error) {
	path := passwordsBranchAPIPath(createReq.Organization, createReq.Database, createReq.Branch)
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
func (d *passwordsService) Delete(ctx context.Context, deleteReq *DeleteDatabaseBranchPasswordRequest) error {
	path := passwordBranchAPIPath(deleteReq.Organization, deleteReq.Database, deleteReq.Branch, deleteReq.PasswordId)
	req, err := d.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return errors.Wrap(err, "error creating http request")
	}

	err = d.client.do(ctx, req, nil)
	return err

}

// Get an existing password for a branch.
func (d *passwordsService) Get(ctx context.Context, getReq *GetDatabaseBranchPasswordRequest) (*DatabaseBranchPassword, error) {
	path := passwordBranchAPIPath(getReq.Organization, getReq.Database, getReq.Branch, getReq.PasswordId)
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

// List all existing passwords. If req.Branch is set, all passwords for that
// branch will be listed.
func (d *passwordsService) List(ctx context.Context, listReq *ListDatabaseBranchPasswordRequest) ([]*DatabaseBranchPassword, error) {
	path := passwordsAPIPath(listReq.Organization, listReq.Database)
	if listReq.Branch != "" {
		path = passwordBranchAPIPath(listReq.Organization, listReq.Database, listReq.Branch, "")
	}

	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request to list passwords")
	}

	passwordsResp := &passwordsResponse{}
	if err := d.client.do(ctx, req, &passwordsResp); err != nil {
		return nil, err
	}

	return passwordsResp.Passwords, nil
}

func passwordBranchAPIPath(org, db, branch, password string) string {
	return fmt.Sprintf("%s/%s", passwordsBranchAPIPath(org, db, branch), password)
}

func passwordsBranchAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/passwords", databaseBranchAPIPath(org, db, branch))
}

func passwordsAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/passwords", databasesAPIPath(org), db)
}

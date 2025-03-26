package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// CreateDatabaseRequest encapsulates the request for creating a new database.
type CreateDatabaseRequest struct {
	Organization string
	Name         string `json:"name"`
	Notes        string `json:"notes,omitempty"`
	Region       string `json:"region,omitempty"`
	ClusterSize  string `json:"cluster_size"`
}

// DatabaseRequest encapsulates the request for getting a single database.
type GetDatabaseRequest struct {
	Organization string
	Database     string
}

// ListDatabasesRequest encapsulates the request for listing all databases in an
// organization.
type ListDatabasesRequest struct {
	Organization string
}

// DeleteDatabaseRequest encapsulates the request for deleting a database from
// an organization.
type DeleteDatabaseRequest struct {
	Organization string
	Database     string
}

// DatabaseService is an interface for communicating with the PlanetScale
// Databases API endpoint.
type DatabasesService interface {
	Create(context.Context, *CreateDatabaseRequest) (*Database, error)
	Get(context.Context, *GetDatabaseRequest) (*Database, error)
	List(context.Context, *ListDatabasesRequest, ...ListOption) ([]*Database, error)
	Delete(context.Context, *DeleteDatabaseRequest) (*DatabaseDeletionRequest, error)
}

// DatabaseDeletionRequest encapsulates the request for deleting a database from
// an organization.
type DatabaseDeletionRequest struct {
	ID    string `json:"id"`
	Actor Actor  `json:"actor"`
}

// DatabaseState represents the state of a database
type DatabaseState string

const (
	DatabasePending         DatabaseState = "pending"
	DatabaseImporting       DatabaseState = "importing"
	DatabaseAwakening       DatabaseState = "awakening"
	DatabaseSleepInProgress DatabaseState = "sleep_in_progress"
	DatabaseSleeping        DatabaseState = "sleeping"
	DatabaseReady           DatabaseState = "ready"
)

// Database represents a PlanetScale database
type Database struct {
	Name      string        `json:"name"`
	Notes     string        `json:"notes"`
	Region    Region        `json:"region"`
	State     DatabaseState `json:"state"`
	HtmlURL   string        `json:"html_url"`
	CreatedAt time.Time     `json:"created_at"`
	UpdatedAt time.Time     `json:"updated_at"`
}

// Database represents a list of PlanetScale databases
type databasesResponse struct {
	Databases []*Database `json:"data"`
}

type databasesService struct {
	client *Client
}

var _ DatabasesService = &databasesService{}

func NewDatabasesService(client *Client) *databasesService {
	return &databasesService{
		client: client,
	}
}

func (ds *databasesService) List(ctx context.Context, listReq *ListDatabasesRequest, opts ...ListOption) ([]*Database, error) {
	path := databasesAPIPath(listReq.Organization)

	defaultOpts := defaultListOptions(WithPerPage(100))
	for _, opt := range opts {
		opt(defaultOpts)
	}

	if vals := defaultOpts.URLValues.Encode(); vals != "" {
		path += "?" + vals
	}

	req, err := ds.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	dbResponse := databasesResponse{}
	err = ds.client.do(ctx, req, &dbResponse)
	if err != nil {
		return nil, err
	}

	return dbResponse.Databases, nil
}

func (ds *databasesService) Create(ctx context.Context, createReq *CreateDatabaseRequest) (*Database, error) {
	req, err := ds.client.newRequest(http.MethodPost, databasesAPIPath(createReq.Organization), createReq)
	if err != nil {
		return nil, fmt.Errorf("error creating request for create database: %w", err)
	}

	db := &Database{}
	err = ds.client.do(ctx, req, &db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (ds *databasesService) Get(ctx context.Context, getReq *GetDatabaseRequest) (*Database, error) {
	path := fmt.Sprintf("%s/%s", databasesAPIPath(getReq.Organization), getReq.Database)
	req, err := ds.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for get database: %w", err)
	}

	db := &Database{}
	err = ds.client.do(ctx, req, &db)
	if err != nil {
		return nil, err
	}

	return db, nil
}

func (ds *databasesService) Delete(ctx context.Context, deleteReq *DeleteDatabaseRequest) (*DatabaseDeletionRequest, error) {
	path := fmt.Sprintf("%s/%s", databasesAPIPath(deleteReq.Organization), deleteReq.Database)
	req, err := ds.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for delete database: %w", err)
	}

	var dbr *DatabaseDeletionRequest
	err = ds.client.do(ctx, req, &dbr)
	if err != nil {
		return nil, err
	}

	return dbr, nil
}

func databasesAPIPath(org string) string {
	return fmt.Sprintf("v1/organizations/%s/databases", org)
}

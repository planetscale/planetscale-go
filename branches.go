package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"reflect"
	"time"

	"github.com/google/jsonapi"
	"github.com/pkg/errors"
)

// CreateDatabaseBranchRequest encapsulates the request for creating a new
// database branch
type CreateDatabaseBranchRequest struct {
	Branch *DatabaseBranch `json:"branch"`
}

// DatabaseBranch represents a database branch
type DatabaseBranch struct {
	Name      string    `jsonapi:"attr,name" json:"name"`
	Notes     string    `jsonapi:"attr,notes" json:"notes"`
	CreatedAt time.Time `jsonapi:"attr,created_at,iso8601" json:"created_at"`
	UpdatedAt time.Time `jsonapi:"attr,updated_at,iso8601" json:"updated_at"`
}

// DatabaseBranchesService is an interface for communicating with the PlanetScale
// Database Branch API endpoint.
type DatabaseBranchesService interface {
	Create(context.Context, string, string, *CreateDatabaseBranchRequest) (*DatabaseBranch, error)
	List(context.Context, string, string) ([]*DatabaseBranch, error)
	Get(context.Context, string, string, string) (*DatabaseBranch, error)
	Delete(context.Context, string, string, string) (bool, error)
	Status(context.Context, string, string, string) (*DatabaseStatus, error)
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

func (ds *databaseBranchesService) Create(ctx context.Context, org, db string, createReq *CreateDatabaseBranchRequest) (*DatabaseBranch, error) {
	path := databaseBranchesAPIPath(org, db)
	req, err := ds.client.newRequest(http.MethodPost, path, createReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for branch database")
	}
	res, err := ds.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	dbBranch := &DatabaseBranch{}
	err = jsonapi.UnmarshalPayload(res.Body, dbBranch)
	if err != nil {
		return nil, err
	}

	return dbBranch, nil
}

func (ds *databaseBranchesService) Get(ctx context.Context, org, db, branch string) (*DatabaseBranch, error) {
	path := fmt.Sprintf("%s/%s", databaseBranchesAPIPath(org, db), branch)
	req, err := ds.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	res, err := ds.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	dbBranch := &DatabaseBranch{}
	err = jsonapi.UnmarshalPayload(res.Body, dbBranch)
	if err != nil {
		return nil, err
	}

	return dbBranch, nil
}

func (ds *databaseBranchesService) List(ctx context.Context, org, db string) ([]*DatabaseBranch, error) {
	req, err := ds.client.newRequest(http.MethodGet, databaseBranchesAPIPath(org, db), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	res, err := ds.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	databases, err := jsonapi.UnmarshalManyPayload(res.Body, reflect.TypeOf(new(DatabaseBranch)))
	if err != nil {
		return nil, err
	}

	dbBranches := make([]*DatabaseBranch, 0, len(databases))

	for _, database := range databases {
		db, ok := database.(*DatabaseBranch)
		if ok {
			dbBranches = append(dbBranches, db)
		}
	}

	return dbBranches, nil
}

func (ds *databaseBranchesService) Delete(ctx context.Context, org, db, branch string) (bool, error) {
	path := fmt.Sprintf("%s/%s", databaseBranchesAPIPath(org, db), branch)
	req, err := ds.client.newRequest(http.MethodDelete, path, nil)
	if err != nil {
		return false, errors.Wrap(err, "error creating request for delete branch")
	}

	res, err := ds.client.Do(ctx, req)
	if err != nil {
		return false, errors.Wrap(err, "error deleting database")
	}
	defer res.Body.Close()

	if res.StatusCode == http.StatusNotFound {
		return false, errors.New("database branch not found")
	}

	return true, nil
}

func (ds *databaseBranchesService) Status(ctx context.Context, org, db, branch string) (*DatabaseStatus, error) {
	return nil, nil
}

func databaseBranchesAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/branches", databasesAPIPath(org), db)
}

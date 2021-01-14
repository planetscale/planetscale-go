package planetscale

import "context"

// CreateDatabaseBranchRequest encapsulates the request for creating a new
// database branch
type CreateDatabaseBranchRequest struct {
	Branch *Database `json:"branch"`
}

// DatabaseBranchesService is an interface for communicating with the PlanetScale
// Database Branch API endpoint.
type DatabaseBranchesService interface {
	Create(context.Context, string, string, *CreateDatabaseBranchRequest) (*Database, error)
	List(context.Context, string, string) ([]*Database, error)
	Get(context.Context, string, string, string) (*Database, error)
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

func (dbs *databaseBranchesService) Create(ctx context.Context, org, db string, req *CreateDatabaseBranchRequest) (*Database, error) {

	return nil, nil
}

func (dbs *databaseBranchesService) List(ctx context.Context, org, db string) ([]*Database, error) {
	return nil, nil
}

func (dbs *databaseBranchesService) Get(ctx context.Context, org, db, branch string) (*Database, error) {
	return nil, nil
}

func (dbs *databaseBranchesService) Delete(ctx context.Context, org, db, branch string) (bool, error) {
	return false, nil
}

func (dbs *databaseBranchesService) Status(ctx context.Context, org, db, branch string) (*DatabaseStatus, error) {
	return nil, nil
}

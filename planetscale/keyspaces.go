package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type Keyspace struct {
	ID            string      `json:"id"`
	Name          string      `json:"name"`
	Shards        int         `json:"shards"`
	Sharded       bool        `json:"sharded"`
	Replicas      uint64      `json:"replicas"`
	ExtraReplicas uint64      `json:"extra_replicas"`
	Resizing      bool        `json:"resizing"`
	ClusterSize   ClusterSize `json:"cluster_rate_name"`
	CreatedAt     time.Time   `json:"created_at"`
	UpdatedAt     time.Time   `json:"updated_at"`
}

// VSchema represnts the VSchema for a branch keyspace
type VSchema struct {
	Raw  string `json:"raw"`
	HTML string `json:"html"`
}

type ListBranchKeyspacesRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

type GetBranchKeyspaceRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
}

type GetKeyspaceVSchemaRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
}

type UpdateKeyspaceVSchemaRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
	VSchema      string `json:"vschema"`
}

type branchKeyspacesResponse struct {
	Keyspaces []*Keyspace `json:"data"`
}

// BranchKeyspaceService is an interface for interacting with the keyspace endpoints of the PlanetScale API
type BranchKeyspacesService interface {
	List(context.Context, *ListBranchKeyspacesRequest) ([]*Keyspace, error)
	Get(context.Context, *GetBranchKeyspaceRequest) (*Keyspace, error)
	VSchema(context.Context, *GetKeyspaceVSchemaRequest) (*VSchema, error)
	UpdateVSchema(context.Context, *UpdateKeyspaceVSchemaRequest) (*VSchema, error)
}

type branchKeyspacesService struct {
	client *Client
}

var _ BranchKeyspacesService = &branchKeyspacesService{}

func NewBranchKeyspacesService(client *Client) *branchKeyspacesService {
	return &branchKeyspacesService{client}
}

// List returns a list of keyspaces for a branch
func (s *branchKeyspacesService) List(ctx context.Context, listReq *ListBranchKeyspacesRequest) ([]*Keyspace, error) {
	req, err := s.client.newRequest(http.MethodGet, databaseBranchKeyspacesAPIPath(listReq.Organization, listReq.Database, listReq.Branch), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	branchKeyspaces := &branchKeyspacesResponse{}
	if err := s.client.do(ctx, req, branchKeyspaces); err != nil {
		return nil, err
	}

	return branchKeyspaces.Keyspaces, nil
}

// Get returns a keyspace for a branch
func (s *branchKeyspacesService) Get(ctx context.Context, getReq *GetBranchKeyspaceRequest) (*Keyspace, error) {
	req, err := s.client.newRequest(http.MethodGet, databaseBranchKeyspaceAPIPath(getReq.Organization, getReq.Database, getReq.Branch, getReq.Keyspace), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	keyspace := &Keyspace{}
	if err := s.client.do(ctx, req, keyspace); err != nil {
		return nil, err
	}

	return keyspace, nil
}

// VSchema returns the VSchema for a keyspace in a branch
func (s *branchKeyspacesService) VSchema(ctx context.Context, getReq *GetKeyspaceVSchemaRequest) (*VSchema, error) {
	path := fmt.Sprintf("%s/vschema", databaseBranchKeyspaceAPIPath(getReq.Organization, getReq.Database, getReq.Branch, getReq.Keyspace))
	req, err := s.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	vschema := &VSchema{}
	if err := s.client.do(ctx, req, vschema); err != nil {
		return nil, err
	}

	return vschema, nil
}

func (s *branchKeyspacesService) UpdateVSchema(ctx context.Context, updateReq *UpdateKeyspaceVSchemaRequest) (*VSchema, error) {
	path := fmt.Sprintf("%s/vschema", databaseBranchKeyspaceAPIPath(updateReq.Organization, updateReq.Database, updateReq.Branch, updateReq.Keyspace))
	req, err := s.client.newRequest(http.MethodPatch, path, updateReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	vschema := &VSchema{}
	if err := s.client.do(ctx, req, vschema); err != nil {
		return nil, err
	}

	return vschema, nil
}

func databaseBranchKeyspacesAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/keyspaces", databaseBranchAPIPath(org, db, branch))
}

func databaseBranchKeyspaceAPIPath(org, db, branch, keyspace string) string {
	return fmt.Sprintf("%s/%s", databaseBranchKeyspacesAPIPath(org, db, branch), keyspace)
}

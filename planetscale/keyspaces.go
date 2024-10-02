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
	ResizePending bool        `json:"resize_pending"`
	Resizing      bool        `json:"resizing"`
	Ready         bool        `json:"ready"`
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

type CreateBranchKeyspaceRequest struct {
	Organization  string      `json:"-"`
	Database      string      `json:"-"`
	Branch        string      `json:"-"`
	Name          string      `json:"name"`
	ClusterSize   ClusterSize `json:"cluster_size"`
	ExtraReplicas int         `json:"replicas"`
	Shards        int         `json:"shards"`
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

type ResizeKeyspaceRequest struct {
	Organization  string      `json:"-"`
	Database      string      `json:"-"`
	Branch        string      `json:"-"`
	Keyspace      string      `json:"-"`
	ExtraReplicas uint        `json:"replicas,omitempty"`
	ClusterSize   ClusterSize `json:"cluster_size,omitempty"`
}

type KeyspaceResizeRequest struct {
	ID    string `json:"id"`
	State string `json:"state"`
	Actor *Actor `json:"actor"`

	ClusterSize         ClusterSize `json:"cluster_rate_name"`
	PreviousClusterSize ClusterSize `json:"previous_cluster_rate_name"`

	Replicas         uint `json:"replicas"`
	ExtraReplicas    uint `json:"extra_replicas"`
	PreviousReplicas uint `json:"previous_replicas"`

	UpdatedAt   time.Time  `json:"updated_at"`
	CreatedAt   time.Time  `json:"created_at"`
	StartedAt   *time.Time `json:"started_at"`
	CompletedAt *time.Time `json:"completed_at"`
}

type CancelKeyspaceResizeRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Keyspace     string `json:"-"`
}

// BranchKeyspaceService is an interface for interacting with the keyspace endpoints of the PlanetScale API
type BranchKeyspacesService interface {
	Create(context.Context, *CreateBranchKeyspaceRequest) (*Keyspace, error)
	List(context.Context, *ListBranchKeyspacesRequest) ([]*Keyspace, error)
	Get(context.Context, *GetBranchKeyspaceRequest) (*Keyspace, error)
	VSchema(context.Context, *GetKeyspaceVSchemaRequest) (*VSchema, error)
	UpdateVSchema(context.Context, *UpdateKeyspaceVSchemaRequest) (*VSchema, error)
	Resize(context.Context, *ResizeKeyspaceRequest) (*KeyspaceResizeRequest, error)
	CancelResize(context.Context, *CancelKeyspaceResizeRequest) error
	// ResizeStatus(context.Context, *KeyspaceResizeStatusRequest) (*KeyspaceResizeRequest, error)
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

// Create creates a keyspace for a branch
func (s *branchKeyspacesService) Create(ctx context.Context, createReq *CreateBranchKeyspaceRequest) (*Keyspace, error) {
	req, err := s.client.newRequest(http.MethodPost, databaseBranchKeyspacesAPIPath(createReq.Organization, createReq.Database, createReq.Branch), createReq)
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

func (s *branchKeyspacesService) Resize(ctx context.Context, resizeReq *ResizeKeyspaceRequest) (*KeyspaceResizeRequest, error) {
	req, err := s.client.newRequest(http.MethodPut, databaseBranchKeyspaceResizesAPIPath(resizeReq.Organization, resizeReq.Database, resizeReq.Branch, resizeReq.Keyspace), resizeReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	keyspaceResize := &KeyspaceResizeRequest{}
	if err := s.client.do(ctx, req, keyspaceResize); err != nil {
		return nil, err
	}

	return keyspaceResize, nil
}

// CancelResize cancels a queued resize of a branch's keyspace.
func (s *branchKeyspacesService) CancelResize(ctx context.Context, cancelReq *CancelKeyspaceResizeRequest) error {
	req, err := s.client.newRequest(http.MethodDelete, databaseBranchKeyspaceResizesAPIPath(cancelReq.Organization, cancelReq.Database, cancelReq.Branch, cancelReq.Keyspace), nil)
	if err != nil {
		return errors.Wrap(err, "error creating http request")
	}

	return s.client.do(ctx, req, nil)
}

func databaseBranchKeyspacesAPIPath(org, db, branch string) string {
	return fmt.Sprintf("%s/keyspaces", databaseBranchAPIPath(org, db, branch))
}

func databaseBranchKeyspaceAPIPath(org, db, branch, keyspace string) string {
	return fmt.Sprintf("%s/%s", databaseBranchKeyspacesAPIPath(org, db, branch), keyspace)
}

func databaseBranchKeyspaceResizesAPIPath(org, db, branch, keyspace string) string {
	return fmt.Sprintf("%s/resizes", databaseBranchKeyspaceAPIPath(org, db, branch, keyspace))
}

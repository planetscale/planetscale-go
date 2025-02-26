package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

const organizationsAPIPath = "v1/organizations"

// GetOrganizationRequest encapsulates the request for getting a single
// organization.
type GetOrganizationRequest struct {
	Organization string
}

// OrganizationsService is an interface for communicating with the PlanetScale
// Organizations API endpoints.
type OrganizationsService interface {
	Get(context.Context, *GetOrganizationRequest) (*Organization, error)
	List(context.Context) ([]*Organization, error)
	ListRegions(context.Context, *ListOrganizationRegionsRequest) ([]*Region, error)
	ListClusterSKUs(context.Context, *ListOrganizationClusterSKUsRequest, ...ListOption) ([]*ClusterSKU, error)
}

// ListRegionsRequest encapsulates the request for getting a list of regions for
// an organization.
type ListOrganizationRegionsRequest struct {
	Organization string
}

// ListOrganizationClusterSKUsRequest encapsulates the request for getting a list of Cluster SKUs for an organization.
type ListOrganizationClusterSKUsRequest struct {
	Organization string
	IncludeRates bool
}

// ClusterSKU represents a SKU for a PlanetScale cluster
type ClusterSKU struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	CPU         string `json:"cpu"`
	Memory      string `json:"ram"`

	Storage *int64 `json:"storage,string"`

	Rate                 *int64  `json:"rate"`
	ReplicaRate          *int64  `json:"replica_rate"`
	ProviderInstanceType *string `json:"provider_instance_type"`
	Provider             *string `json:"provider"`
	Enabled              bool    `json:"enabled"`
	DefaultVTGate        string  `json:"default_vtgate"`
	DefaultVTGateRate    *int64  `json:"default_vtgate_rate"`
}

// Organization represents a PlanetScale organization.
type Organization struct {
	Name                   string    `json:"name"`
	CreatedAt              time.Time `json:"created_at"`
	UpdatedAt              time.Time `json:"updated_at"`
	RemainingFreeDatabases int       `json:"free_databases_remaining"`
}

type organizationsResponse struct {
	Organizations []*Organization `json:"data"`
}

type organizationsService struct {
	client *Client
}

var _ OrganizationsService = &organizationsService{}

func NewOrganizationsService(client *Client) *organizationsService {
	return &organizationsService{
		client: client,
	}
}

// Get fetches a single organization by name.
func (o *organizationsService) Get(ctx context.Context, getReq *GetOrganizationRequest) (*Organization, error) {
	req, err := o.client.newRequest(http.MethodGet, fmt.Sprintf("%s/%s", organizationsAPIPath, getReq.Organization), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for get organization")
	}

	org := &Organization{}
	if err := o.client.do(ctx, req, &org); err != nil {
		return nil, err
	}

	return org, nil
}

// List returns all the organizations for a user.
func (o *organizationsService) List(ctx context.Context) ([]*Organization, error) {
	req, err := o.client.newRequest(http.MethodGet, organizationsAPIPath, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for list organization")
	}

	orgResponse := &organizationsResponse{}
	if err := o.client.do(ctx, req, &orgResponse); err != nil {
		return nil, err
	}

	return orgResponse.Organizations, nil
}

type listRegionsResponse struct {
	Regions []*Region `json:"data"`
}

func (o *organizationsService) ListRegions(ctx context.Context, listReq *ListOrganizationRegionsRequest) ([]*Region, error) {
	req, err := o.client.newRequest(http.MethodGet, fmt.Sprintf("%s/%s/regions", organizationsAPIPath, listReq.Organization), nil)
	if err != nil {
		return nil, err
	}

	listResponse := &listRegionsResponse{}
	if err := o.client.do(ctx, req, &listResponse); err != nil {
		return nil, err
	}

	return listResponse.Regions, nil
}

func (o *organizationsService) ListClusterSKUs(ctx context.Context, listReq *ListOrganizationClusterSKUsRequest, opts ...ListOption) ([]*ClusterSKU, error) {
	path := fmt.Sprintf("%s/%s/cluster-size-skus", organizationsAPIPath, listReq.Organization)

	defaultOpts := defaultListOptions()
	for _, opt := range opts {
		opt(defaultOpts)
	}

	if vals := defaultOpts.URLValues.Encode(); vals != "" {
		path += "?" + vals
	}

	req, err := o.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	clusterSKUs := []*ClusterSKU{}
	if err := o.client.do(ctx, req, &clusterSKUs); err != nil {
		return nil, err
	}

	return clusterSKUs, nil
}

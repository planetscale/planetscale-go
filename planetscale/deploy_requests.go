package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

// PerformDeployRequest is a request for approving and deploying a deploy request.
// NOTE: We deviate from naming convention here because we have a data model
// named DeployRequest already.
type PerformDeployRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Number       uint64 `json:"-"`
}

// GetDeployRequest encapsulates the request for getting a single deploy
// request.
type GetDeployRequestRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Number       uint64 `json:"-"`
}

// ListDeployRequestsRequest gets the deploy requests for a specific database
// branch.
type ListDeployRequestsRequest struct {
	Organization string
	Database     string
}

// DeployRequest encapsulates a requested deploy of a schema snapshot.
type DeployRequest struct {
	ID string `json:"id"`

	Number uint64 `json:"number"`

	DeployabilityErrors string `json:"deployability_errors"`
	DeploymentState     string `json:"deployment_state"`
	Ready               bool   `json:"ready"`
	Approved            bool   `json:"approved"`
	Deployed            bool   `json:"deployed"`

	Branch     string `json:"branch"`
	IntoBranch string `json:"into_branch"`

	Notes string `json:"notes"`

	CreatedAt time.Time  `json:"created_at"`
	UpdatedAt time.Time  `json:"updated_at"`
	ClosedAt  *time.Time `json:"closed_at"`
}

type CancelDeployRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Number       uint64 `json:"-"`
}

// DeployRequestsService is an interface for communicating with the PlanetScale
// deploy requests API.
type DeployRequestsService interface {
	List(context.Context, *ListDeployRequestsRequest) ([]*DeployRequest, error)
	Get(context.Context, *GetDeployRequestRequest) (*DeployRequest, error)
	Deploy(context.Context, *PerformDeployRequest) (*DeployRequest, error)
	CancelDeploy(context.Context, *CancelDeployRequest) (*DeployRequest, error)
}

type deployRequestsService struct {
	client *Client
}

var _ DeployRequestsService = &deployRequestsService{}

func NewDeployRequestsService(client *Client) *deployRequestsService {
	return &deployRequestsService{
		client: client,
	}
}

// Get fetches a single deploy request.
func (d *deployRequestsService) Get(ctx context.Context, getReq *GetDeployRequestRequest) (*DeployRequest, error) {
	req, err := d.client.newRequest(http.MethodGet, deployRequestAPIPath(getReq.Organization, getReq.Database, getReq.Number), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	res, err := d.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	dr := &DeployRequest{}
	err = json.NewDecoder(res.Body).Decode(dr)
	if err != nil {
		return nil, err
	}

	return dr, nil
}

// Deploy approves and executes a specific deploy request.
func (d *deployRequestsService) Deploy(ctx context.Context, deployReq *PerformDeployRequest) (*DeployRequest, error) {
	path := fmt.Sprintf("%s/deploy", deployRequestAPIPath(deployReq.Organization, deployReq.Database, deployReq.Number))
	req, err := d.client.newRequest(http.MethodPost, path, deployReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	res, err := d.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	dr := &DeployRequest{}
	err = json.NewDecoder(res.Body).Decode(dr)
	if err != nil {
		return nil, err
	}

	return dr, nil
}

type deployRequestsResponse struct {
	DeployRequests []*DeployRequest `json:"data"`
}

// CancelDeploy approves and executes a specific deploy request.
func (d *deployRequestsService) CancelDeploy(ctx context.Context, deployReq *CancelDeployRequest) (*DeployRequest, error) {
	path := fmt.Sprintf("%s/cancel", deployRequestAPIPath(deployReq.Organization, deployReq.Database, deployReq.Number))
	req, err := d.client.newRequest(http.MethodPost, path, deployReq)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	res, err := d.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	dr := &DeployRequest{}
	err = json.NewDecoder(res.Body).Decode(dr)
	if err != nil {
		return nil, err
	}

	return dr, nil
}

func (d *deployRequestsService) List(ctx context.Context, listReq *ListDeployRequestsRequest) ([]*DeployRequest, error) {
	req, err := d.client.newRequest(http.MethodGet, deployRequestsAPIPath(listReq.Organization, listReq.Database), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	res, err := d.client.Do(ctx, req)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	deployRequests := &deployRequestsResponse{}
	err = json.NewDecoder(res.Body).Decode(&deployRequests)

	if err != nil {
		return nil, err
	}

	return deployRequests.DeployRequests, nil
}

func deployRequestsAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/deploy-requests", databasesAPIPath(org), db)
}

// deployRequestAPIPath gets the base path for accessing a single deploy request
func deployRequestAPIPath(org string, db string, number uint64) string {
	return fmt.Sprintf("%s/%s/deploy-requests/%d", databasesAPIPath(org), db, number)
}

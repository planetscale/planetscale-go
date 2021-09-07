package planetscale

import (
	"context"
	"net/http"

	"github.com/pkg/errors"
)

const regionsAPIPath = "v1/regions"

type Region struct {
	Slug     string `json:"slug"`
	Name     string `json:"display_name"`
	Location string `json:"location"`
	Enabled  bool   `json:"enabled"`
}

type regionsResponse struct {
	Regions []*Region `json:"data"`
}

type ListRegionsRequest struct{}

type RegionsService interface {
	List(ctx context.Context, req *ListRegionsRequest) ([]*Region, error)
}

type regionsService struct {
	client *Client
}

var _ RegionsService = &regionsService{}

func NewRegionsSevice(client *Client) *regionsService {
	return &regionsService{
		client: client,
	}
}

func (r *regionsService) List(ctx context.Context, listReq *ListRegionsRequest) ([]*Region, error) {
	req, err := r.client.newRequest(http.MethodGet, regionsAPIPath, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for list regions")
	}

	regionsResponse := &regionsResponse{}
	if err := r.client.do(ctx, req, &regionsResponse); err != nil {
		return nil, err
	}

	return regionsResponse.Regions, nil
}

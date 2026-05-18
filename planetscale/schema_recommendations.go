package planetscale

import (
	"context"
	"net/http"
	"path"
)

var _ SchemaRecommendationService = &schemaRecommendationService{}

// SchemaRecommendationService is an interface for communicating with the PlanetScale
// Schema recommendation API.
type SchemaRecommendationService interface {
	List(context.Context, *ListSchemaRecommendationsRequest, ...ListOption) ([]*SchemaRecommendation, error)
	Get(context.Context, *GetSchemaRecommendationRequest) (*SchemaRecommendation, error)
	Dismiss(context.Context, *DismissSchemaRecommendationRequest) (*SchemaRecommendation, error)
}

type schemaRecommendationsResponse struct {
	SchemaRecommendations []*SchemaRecommendation `json:"data"`
}

// SchemaRecommendation represents a PlanetScale schema recommendation.
type SchemaRecommendation struct {
	Id                    string `json:"id"`
	HtmlUrl               string `json:"html_url"`
	Title                 string `json:"title"`
	TableName             string `json:"table_name"`
	Keyspace              string `json:"keyspace"`
	DdlStatement          string `json:"ddl_statement"`
	Number                int    `json:"number"`
	State                 string `json:"state"`
	RecommendationType    string `json:"recommendation_type"`
	CreatedAt             string `json:"created_at"`
	UpdatedAt             string `json:"updated_at"`
	AppliedAt             string `json:"applied_at"`
	DismissedAt           string `json:"dismissed_at"`
	ClosedByDeployRequest struct {
		Id       string `json:"id"`
		BranchId string `json:"branch_id"`
		Number   int    `json:"number"`
	} `json:"closed_by_deploy_request"`
	DismissedBy struct {
		Id          string `json:"id"`
		DisplayName string `json:"display_name"`
		AvatarUrl   string `json:"avatar_url"`
	} `json:"dismissed_by"`
}

// ListSchemaRecommendationsRequest is the request for listing schema recommendations.
type ListSchemaRecommendationsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
}

// GetchemaRecommendationRequest is the request for getting a schema recommendation.
type GetSchemaRecommendationRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	ID           string `json:"-"`
}

// DismissSchemaRecommendationRequest is the request for dismissing a schema recommendation.
type DismissSchemaRecommendationRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	ID           string `json:"-"`
}

type schemaRecommendationService struct {
	client *Client
}

func (s *schemaRecommendationService) List(ctx context.Context, request *ListSchemaRecommendationsRequest, opts ...ListOption) ([]*SchemaRecommendation, error) {
	listOpts := defaultListOptions(opts...)

	req, err := s.client.newRequest(http.MethodGet, schemaRecommendationsAPIPath(request.Organization, request.Database), nil, WithQueryParams(*listOpts.URLValues))
	if err != nil {
		return nil, err
	}

	resp := &schemaRecommendationsResponse{}
	if err := s.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	return resp.SchemaRecommendations, nil
}

func (s *schemaRecommendationService) Get(ctx context.Context, request *GetSchemaRecommendationRequest) (*SchemaRecommendation, error) {
	req, err := s.client.newRequest(http.MethodGet, schemaRecommendationAPIPath(request.Organization, request.Database, request.ID), nil)
	if err != nil {
		return nil, err
	}

	schemaRecommendation := &SchemaRecommendation{}
	if err := s.client.do(ctx, req, &schemaRecommendation); err != nil {
		return nil, err
	}

	return schemaRecommendation, nil
}

func (s *schemaRecommendationService) Dismiss(ctx context.Context, request *DismissSchemaRecommendationRequest) (*SchemaRecommendation, error) {
	req, err := s.client.newRequest(http.MethodPost, dismissSchemaRecommendationAPIPath(request.Organization, request.Database, request.ID), nil)
	if err != nil {
		return nil, err
	}

	schemaRecommendation := &SchemaRecommendation{}
	if err := s.client.do(ctx, req, &schemaRecommendation); err != nil {
		return nil, err
	}

	return schemaRecommendation, nil
}

func schemaRecommendationsAPIPath(org, db string) string {
	return path.Join("v1/organizations", org, "databases", db, "schema-recommendations")
}

func schemaRecommendationAPIPath(org, db, id string) string {
	return path.Join(schemaRecommendationsAPIPath(org, db), id)
}

func dismissSchemaRecommendationAPIPath(org, db, id string) string {
	return path.Join(schemaRecommendationAPIPath(org, db, id), "dismiss")
}

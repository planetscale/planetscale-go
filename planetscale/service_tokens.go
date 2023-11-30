package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
)

var _ ServiceTokenService = &serviceTokenService{}

// ServiceTokenService is an interface for communicating with the PlanetScale
// Service Token API.
type ServiceTokenService interface {
	Create(context.Context, *CreateServiceTokenRequest) (*ServiceToken, error)
	List(context.Context, *ListServiceTokensRequest) ([]*ServiceToken, error)
	ListGrants(context.Context, *ListServiceTokenGrantsRequest) ([]*ServiceTokenGrant, error)
	Delete(context.Context, *DeleteServiceTokenRequest) error
	GetAccess(context.Context, *GetServiceTokenAccessRequest) ([]*ServiceTokenAccess, error)
	AddAccess(context.Context, *AddServiceTokenAccessRequest) ([]*ServiceTokenAccess, error)
	DeleteAccess(context.Context, *DeleteServiceTokenAccessRequest) error
}

type serviceTokenService struct {
	client *Client
}

func (s *serviceTokenService) Create(ctx context.Context, createReq *CreateServiceTokenRequest) (*ServiceToken, error) {
	req, err := s.client.newRequest(http.MethodPost, serviceTokensAPIPath(createReq.Organization), nil)
	if err != nil {
		return nil, err
	}

	st := &ServiceToken{}
	if err := s.client.do(ctx, req, &st); err != nil {
		return nil, err
	}

	return st, nil
}

func (s *serviceTokenService) List(ctx context.Context, listReq *ListServiceTokensRequest) ([]*ServiceToken, error) {
	req, err := s.client.newRequest(http.MethodGet, serviceTokensAPIPath(listReq.Organization), nil)
	if err != nil {
		return nil, err
	}

	tokenListResponse := serviceTokensResponse{}
	if err := s.client.do(ctx, req, &tokenListResponse); err != nil {
		return nil, err
	}

	return tokenListResponse.ServiceTokens, nil
}

func (s *serviceTokenService) Delete(ctx context.Context, delReq *DeleteServiceTokenRequest) error {
	req, err := s.client.newRequest(http.MethodDelete, serviceTokenAPIPath(delReq.Organization, delReq.ID), nil)
	if err != nil {
		return err
	}

	err = s.client.do(ctx, req, nil)
	return err
}

func (s *serviceTokenService) GetAccess(ctx context.Context, accessReq *GetServiceTokenAccessRequest) ([]*ServiceTokenAccess, error) {
	req, err := s.client.newRequest(http.MethodGet, serviceTokenAccessAPIPath(accessReq.Organization, accessReq.ID), nil)
	if err != nil {
		return nil, err
	}

	tokenAccess := serviceTokenAccessResponse{}
	if err := s.client.do(ctx, req, &tokenAccess); err != nil {
		return nil, err
	}
	return tokenAccess.ServiceTokenAccesses, nil
}

func (s *serviceTokenService) ListGrants(ctx context.Context, listReq *ListServiceTokenGrantsRequest) ([]*ServiceTokenGrant, error) {
	req, err := s.client.newRequest(http.MethodGet, serviceTokenGrantsAPIPath(listReq.Organization, listReq.ID), nil)
	if err != nil {
		return nil, err
	}

	tokenGrants := serviceTokenGrantsResponse{}
	if err := s.client.do(ctx, req, &tokenGrants); err != nil {
		return nil, err
	}
	return tokenGrants.ServiceTokenGrants, nil
}

func (s *serviceTokenService) AddAccess(ctx context.Context, addReq *AddServiceTokenAccessRequest) ([]*ServiceTokenAccess, error) {
	req, err := s.client.newRequest(http.MethodPost, serviceTokenAccessAPIPath(addReq.Organization, addReq.ID), addReq)
	if err != nil {
		return nil, err
	}

	tokenAccess := serviceTokenAccessResponse{}
	if err := s.client.do(ctx, req, &tokenAccess); err != nil {
		return nil, err
	}
	return tokenAccess.ServiceTokenAccesses, nil
}

func (s *serviceTokenService) DeleteAccess(ctx context.Context, delReq *DeleteServiceTokenAccessRequest) error {
	req, err := s.client.newRequest(http.MethodDelete, serviceTokenAccessAPIPath(delReq.Organization, delReq.ID), delReq)
	if err != nil {
		return err
	}

	err = s.client.do(ctx, req, nil)
	return err
}

type CreateServiceTokenRequest struct {
	Organization string `json:"-"`
}

type ListServiceTokenGrantsRequest struct {
	Organization string `json:"-"`
	ID           string `json:"-"`
}

type DeleteServiceTokenRequest struct {
	Organization string `json:"-"`
	ID           string `json:"-"`
}

type ListServiceTokensRequest struct {
	Organization string `json:"-"`
}

type GetServiceTokenAccessRequest struct {
	Organization string `json:"-"`
	ID           string `json:"-"`
}

type AddServiceTokenAccessRequest struct {
	Organization string   `json:"-"`
	ID           string   `json:"-"`
	Database     string   `json:"database"`
	Accesses     []string `json:"access"`
}

type DeleteServiceTokenAccessRequest struct {
	Organization string   `json:"-"`
	ID           string   `json:"-"`
	Database     string   `json:"database"`
	Accesses     []string `json:"access"`
}

type ServiceToken struct {
	ID    string `json:"id"`
	Type  string `json:"type"`
	Token string `json:"token"`
}

type ServiceTokenGrant struct {
	ID           string                     `json:"id"`
	ResourceName string                     `json:"resource_name"`
	ResourceType string                     `json:"resource_type"`
	ResourceID   string                     `json:"resource_id"`
	Accesses     []*ServiceTokenGrantAccess `json:"accesses"`
}

type ServiceTokenGrantAccess struct {
	Access      string `json:"access"`
	Description string `json:"description"`
}

type serviceTokensResponse struct {
	ServiceTokens []*ServiceToken `json:"data"`
}

type ServiceTokenAccess struct {
	ID       string                     `json:"id"`
	Access   string                     `json:"access"`
	Type     string                     `json:"type"`
	Resource ServiceTokenAccessResource `json:"resource"`
}

func (d Database) isServiceTokenAccessResource()     {}
func (o Organization) isServiceTokenAccessResource() {}

func (st *ServiceTokenAccess) UnmarshalJSON(data []byte) error {
	type tempStruct struct {
		ID       string `json:"id"`
		Access   string `json:"access"`
		Type     string `json:"type"`
		Resource json.RawMessage
	}

	var temp tempStruct
	if err := json.Unmarshal(data, &temp); err != nil {
		return err
	}

	st.ID = temp.ID
	st.Access = temp.Access
	st.Type = temp.Type

	var resourceType struct {
		Type string `json:"type"`
	}

	if err := json.Unmarshal(temp.Resource, &resourceType); err != nil {
		return err
	}

	switch resourceType.Type {
	case "Database":
		var db Database
		if err := json.Unmarshal(temp.Resource, &db); err != nil {
			return err
		}
		st.Resource = db
	case "Organization":
		var org Organization
		if err := json.Unmarshal(temp.Resource, &org); err != nil {
			return err
		}
		st.Resource = org
	default:
		return fmt.Errorf("unknown resource type: %s", resourceType.Type)
	}

	return nil
}

type ServiceTokenAccessResource interface {
	isServiceTokenAccessResource()
}

type serviceTokenAccessResponse struct {
	ServiceTokenAccesses []*ServiceTokenAccess `json:"data"`
}

type serviceTokenGrantsResponse struct {
	ServiceTokenGrants []*ServiceTokenGrant `json:"data"`
}

func serviceTokenAccessAPIPath(org, id string) string {
	return fmt.Sprintf("%s/%s/access", serviceTokensAPIPath(org), id)
}

func serviceTokenGrantsAPIPath(org, id string) string {
	return fmt.Sprintf("%s/%s/grants", serviceTokensAPIPath(org), id)
}

func serviceTokensAPIPath(org string) string {
	return fmt.Sprintf("v1/organizations/%s/service-tokens", org)
}

func serviceTokenAPIPath(org, id string) string {
	return fmt.Sprintf("%s/%s", serviceTokensAPIPath(org), id)
}

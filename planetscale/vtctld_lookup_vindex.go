package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

// LookupVindexService is an interface for interacting with the LookupVindex endpoints of the
// PlanetScale API.
type LookupVindexService interface {
	Create(context.Context, *LookupVindexCreateRequest) (json.RawMessage, error)
	Show(context.Context, *LookupVindexShowRequest) (json.RawMessage, error)
	Externalize(context.Context, *LookupVindexExternalizeRequest) (json.RawMessage, error)
	Internalize(context.Context, *LookupVindexInternalizeRequest) (json.RawMessage, error)
	Cancel(context.Context, *LookupVindexCancelRequest) (json.RawMessage, error)
	Complete(context.Context, *LookupVindexCompleteRequest) (json.RawMessage, error)
}

type LookupVindexCreateRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`

	Name                         string   `json:"name"`
	TableKeyspace                string   `json:"table_keyspace"`
	Keyspace                     string   `json:"keyspace,omitempty"`
	TableOwner                   string   `json:"table_owner,omitempty"`
	TableName                    string   `json:"table_name,omitempty"`
	Type                         string   `json:"type,omitempty"`
	Cells                        []string `json:"cells,omitempty"`
	TabletTypes                  []string `json:"tablet_types,omitempty"`
	TabletTypesInPreferenceOrder *bool    `json:"tablet_types_in_preference_order,omitempty"`
	IgnoreNulls                  *bool    `json:"ignore_nulls,omitempty"`
	TableOwnerColumns            []string `json:"table_owner_columns,omitempty"`
	TableVindexType              string   `json:"table_vindex_type,omitempty"`
	ContinueAfterCopyWithOwner   *bool    `json:"continue_after_copy_with_owner,omitempty"`
}

type LookupVindexShowRequest struct {
	Organization  string `json:"-"`
	Database      string `json:"-"`
	Branch        string `json:"-"`
	Name          string `json:"-"`
	TableKeyspace string `json:"-"`
}

type LookupVindexExternalizeRequest struct {
	Organization  string `json:"-"`
	Database      string `json:"-"`
	Branch        string `json:"-"`
	Name          string `json:"-"`
	TableKeyspace string `json:"table_keyspace"`
	Keyspace      string `json:"keyspace,omitempty"`
	Delete        *bool  `json:"delete,omitempty"`
}

type LookupVindexInternalizeRequest struct {
	Organization  string `json:"-"`
	Database      string `json:"-"`
	Branch        string `json:"-"`
	Name          string `json:"-"`
	TableKeyspace string `json:"table_keyspace"`
	Keyspace      string `json:"keyspace,omitempty"`
}

type LookupVindexCancelRequest struct {
	Organization  string `json:"-"`
	Database      string `json:"-"`
	Branch        string `json:"-"`
	Name          string `json:"-"`
	TableKeyspace string `json:"table_keyspace"`
}

type LookupVindexCompleteRequest struct {
	Organization  string `json:"-"`
	Database      string `json:"-"`
	Branch        string `json:"-"`
	Name          string `json:"-"`
	TableKeyspace string `json:"table_keyspace"`
	Keyspace      string `json:"keyspace,omitempty"`
}

type lookupVindexService struct {
	client *Client
}

var _ LookupVindexService = &lookupVindexService{}

func lookupVindexVindexesAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "lookup-vindex", "vindexes")
}

func lookupVindexAPIPath(org, db, branch, name string) string {
	return path.Join(lookupVindexVindexesAPIPath(org, db, branch), name)
}

func (s *lookupVindexService) Create(ctx context.Context, req *LookupVindexCreateRequest) (json.RawMessage, error) {
	p := lookupVindexVindexesAPIPath(req.Organization, req.Database, req.Branch)
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *lookupVindexService) Show(ctx context.Context, req *LookupVindexShowRequest) (json.RawMessage, error) {
	p := lookupVindexAPIPath(req.Organization, req.Database, req.Branch, req.Name)
	v := url.Values{}
	v.Set("table_keyspace", req.TableKeyspace)
	httpReq, err := s.client.newRequest(http.MethodGet, p, nil, WithQueryParams(v))
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *lookupVindexService) Externalize(ctx context.Context, req *LookupVindexExternalizeRequest) (json.RawMessage, error) {
	p := path.Join(lookupVindexAPIPath(req.Organization, req.Database, req.Branch, req.Name), "externalize")
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *lookupVindexService) Internalize(ctx context.Context, req *LookupVindexInternalizeRequest) (json.RawMessage, error) {
	p := path.Join(lookupVindexAPIPath(req.Organization, req.Database, req.Branch, req.Name), "internalize")
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *lookupVindexService) Cancel(ctx context.Context, req *LookupVindexCancelRequest) (json.RawMessage, error) {
	p := path.Join(lookupVindexAPIPath(req.Organization, req.Database, req.Branch, req.Name), "cancel")
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

func (s *lookupVindexService) Complete(ctx context.Context, req *LookupVindexCompleteRequest) (json.RawMessage, error) {
	p := path.Join(lookupVindexAPIPath(req.Organization, req.Database, req.Branch, req.Name), "complete")
	httpReq, err := s.client.newRequest(http.MethodPost, p, req)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

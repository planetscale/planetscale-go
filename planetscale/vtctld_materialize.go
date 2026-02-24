package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strconv"
)

// MaterializeService is an interface for interacting with the Materialize endpoints of the
// PlanetScale API.
type MaterializeService interface {
	Create(context.Context, *MaterializeCreateRequest) (json.RawMessage, error)
	Show(context.Context, *MaterializeShowRequest) (json.RawMessage, error)
	Start(context.Context, *MaterializeStartRequest) (json.RawMessage, error)
	Stop(context.Context, *MaterializeStopRequest) (json.RawMessage, error)
	Cancel(context.Context, *MaterializeCancelRequest) (json.RawMessage, error)
}

// MaterializeCreateRequest is a request for creating a Materialize workflow.
type MaterializeCreateRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`

	Workflow                     string          `json:"workflow"`
	TargetKeyspace               string          `json:"target_keyspace"`
	SourceKeyspace               string          `json:"source_keyspace"`
	TableSettings                json.RawMessage `json:"table_settings"`
	Cells                        []string        `json:"cells,omitempty"`
	ReferenceTables              []string        `json:"reference_tables,omitempty"`
	TabletTypes                  []string        `json:"tablet_types,omitempty"`
	StopAfterCopy                *bool           `json:"stop_after_copy,omitempty"`
	TabletTypesInPreferenceOrder *bool           `json:"tablet_types_in_preference_order,omitempty"`
	DeferSecondaryKeys           *bool           `json:"defer_secondary_keys,omitempty"`
	AtomicCopy                   *bool           `json:"atomic_copy,omitempty"`
	OnDDL                        string          `json:"on_ddl,omitempty"`
	SourceTimeZone               string          `json:"source_time_zone,omitempty"`
}

// MaterializeShowRequest is a request for showing a Materialize workflow.
type MaterializeShowRequest struct {
	Organization   string `json:"-"`
	Database       string `json:"-"`
	Branch         string `json:"-"`
	Workflow       string `json:"-"`
	TargetKeyspace string `json:"-"`
	IncludeLogs    *bool  `json:"-"`
}

// MaterializeStartRequest is a request for starting a Materialize workflow.
type MaterializeStartRequest struct {
	Organization   string `json:"-"`
	Database       string `json:"-"`
	Branch         string `json:"-"`
	Workflow       string `json:"-"`
	TargetKeyspace string `json:"target_keyspace"`
}

// MaterializeStopRequest is a request for stopping a Materialize workflow.
type MaterializeStopRequest struct {
	Organization   string `json:"-"`
	Database       string `json:"-"`
	Branch         string `json:"-"`
	Workflow       string `json:"-"`
	TargetKeyspace string `json:"target_keyspace"`
}

// MaterializeCancelRequest is a request for canceling a Materialize workflow.
type MaterializeCancelRequest struct {
	Organization     string `json:"-"`
	Database         string `json:"-"`
	Branch           string `json:"-"`
	Workflow         string `json:"-"`
	TargetKeyspace   string `json:"target_keyspace"`
	KeepData         *bool  `json:"keep_data,omitempty"`
	KeepRoutingRules *bool  `json:"keep_routing_rules,omitempty"`
}

type materializeService struct {
	client *Client
}

var _ MaterializeService = &materializeService{}

func materializeWorkflowsAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "materialize", "workflows")
}

func materializeWorkflowAPIPath(org, db, branch, workflow string) string {
	return path.Join(materializeWorkflowsAPIPath(org, db, branch), workflow)
}

func (s *materializeService) Create(ctx context.Context, req *MaterializeCreateRequest) (json.RawMessage, error) {
	p := materializeWorkflowsAPIPath(req.Organization, req.Database, req.Branch)
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

func (s *materializeService) Show(ctx context.Context, req *MaterializeShowRequest) (json.RawMessage, error) {
	p := materializeWorkflowAPIPath(req.Organization, req.Database, req.Branch, req.Workflow)
	v := url.Values{}
	v.Set("target_keyspace", req.TargetKeyspace)
	if req.IncludeLogs != nil {
		v.Set("include_logs", strconv.FormatBool(*req.IncludeLogs))
	}
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

func (s *materializeService) Start(ctx context.Context, req *MaterializeStartRequest) (json.RawMessage, error) {
	p := path.Join(materializeWorkflowAPIPath(req.Organization, req.Database, req.Branch, req.Workflow), "start")
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

func (s *materializeService) Stop(ctx context.Context, req *MaterializeStopRequest) (json.RawMessage, error) {
	p := path.Join(materializeWorkflowAPIPath(req.Organization, req.Database, req.Branch, req.Workflow), "stop")
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

func (s *materializeService) Cancel(ctx context.Context, req *MaterializeCancelRequest) (json.RawMessage, error) {
	p := path.Join(materializeWorkflowAPIPath(req.Organization, req.Database, req.Branch, req.Workflow), "cancel")
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

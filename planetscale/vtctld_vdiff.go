package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
)

// VDiffService is an interface for interacting with the VDiff endpoints of the
// PlanetScale API.
type VDiffService interface {
	Create(context.Context, *VDiffCreateRequest) (json.RawMessage, error)
	Show(context.Context, *VDiffShowRequest) (json.RawMessage, error)
	Stop(context.Context, *VDiffStopRequest) (json.RawMessage, error)
	Resume(context.Context, *VDiffResumeRequest) (json.RawMessage, error)
	Delete(context.Context, *VDiffDeleteRequest) (json.RawMessage, error)
}

type VDiffCreateRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
	Workflow     string `json:"-"`

	TargetKeyspace              string   `json:"target_keyspace"`
	AutoRetry                   *bool    `json:"auto_retry,omitempty"`
	AutoStart                   *bool    `json:"auto_start,omitempty"`
	DebugQuery                  bool     `json:"debug_query,omitempty"`
	OnlyPKs                     bool     `json:"only_pks,omitempty"`
	UpdateTableStats            bool     `json:"update_table_stats,omitempty"`
	Verbose                     bool     `json:"verbose,omitempty"`
	Tables                      []string `json:"tables,omitempty"`
	TabletTypes                 []string `json:"tablet_types,omitempty"`
	TabletSelectionPreference   string   `json:"tablet_selection_preference,omitempty"`
	FilteredReplicationWaitTime *int     `json:"filtered_replication_wait_time,omitempty"`
	MaxReportSampleRows         *int     `json:"max_report_sample_rows,omitempty"`
	MaxExtraRowsToCompare       *int     `json:"max_extra_rows_to_compare,omitempty"`
	RowDiffColumnTruncateAt     *int     `json:"row_diff_column_truncate_at,omitempty"`
	Limit                       *int     `json:"limit,omitempty"`
}

type VDiffShowRequest struct {
	Organization   string `json:"-"`
	Database       string `json:"-"`
	Branch         string `json:"-"`
	Workflow       string `json:"-"`
	UUID           string `json:"-"`
	TargetKeyspace string `json:"-"`
}

type VDiffStopRequest struct {
	Organization   string   `json:"-"`
	Database       string   `json:"-"`
	Branch         string   `json:"-"`
	Workflow       string   `json:"-"`
	UUID           string   `json:"-"`
	TargetKeyspace string   `json:"target_keyspace"`
	TargetShards   []string `json:"target_shards,omitempty"`
}

type VDiffResumeRequest struct {
	Organization   string   `json:"-"`
	Database       string   `json:"-"`
	Branch         string   `json:"-"`
	Workflow       string   `json:"-"`
	UUID           string   `json:"-"`
	TargetKeyspace string   `json:"target_keyspace"`
	TargetShards   []string `json:"target_shards,omitempty"`
}

type VDiffDeleteRequest struct {
	Organization   string `json:"-"`
	Database       string `json:"-"`
	Branch         string `json:"-"`
	Workflow       string `json:"-"`
	UUID           string `json:"-"`
	TargetKeyspace string `json:"-"`
}

type vdiffService struct {
	client *Client
}

var _ VDiffService = &vdiffService{}

func vdiffVDiffsAPIPath(org, db, branch, workflow string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "vdiff", "workflows", workflow, "vdiffs")
}

func vdiffVDiffAPIPath(org, db, branch, workflow, uuid string) string {
	return path.Join(vdiffVDiffsAPIPath(org, db, branch, workflow), uuid)
}

func (s *vdiffService) Create(ctx context.Context, req *VDiffCreateRequest) (json.RawMessage, error) {
	p := vdiffVDiffsAPIPath(req.Organization, req.Database, req.Branch, req.Workflow)
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

func (s *vdiffService) Show(ctx context.Context, req *VDiffShowRequest) (json.RawMessage, error) {
	p := vdiffVDiffAPIPath(req.Organization, req.Database, req.Branch, req.Workflow, req.UUID)
	v := url.Values{}
	v.Set("target_keyspace", req.TargetKeyspace)
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

func (s *vdiffService) Stop(ctx context.Context, req *VDiffStopRequest) (json.RawMessage, error) {
	p := path.Join(vdiffVDiffAPIPath(req.Organization, req.Database, req.Branch, req.Workflow, req.UUID), "stop")
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

func (s *vdiffService) Resume(ctx context.Context, req *VDiffResumeRequest) (json.RawMessage, error) {
	p := path.Join(vdiffVDiffAPIPath(req.Organization, req.Database, req.Branch, req.Workflow, req.UUID), "resume")
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

func (s *vdiffService) Delete(ctx context.Context, req *VDiffDeleteRequest) (json.RawMessage, error) {
	p := vdiffVDiffAPIPath(req.Organization, req.Database, req.Branch, req.Workflow, req.UUID)
	v := url.Values{}
	v.Set("target_keyspace", req.TargetKeyspace)
	httpReq, err := s.client.newRequest(http.MethodDelete, p, nil, WithQueryParams(v))
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}
	resp := &vtctldDataResponse{}
	if err := s.client.do(ctx, httpReq, resp); err != nil {
		return nil, err
	}
	return resp.Data, nil
}

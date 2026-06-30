package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"path"
)

type D1ImportNotificationsService interface {
	Create(ctx context.Context, req *CreateD1ImportNotificationRequest) error
}

type CreateD1ImportNotificationRequest struct {
	Organization string
	Database     string
	BranchName   string
	MigrationID  string
	Event        string
	Method       string
	ExportBytes  int64
	TableCount   int
	Matched      *bool
	DurationMs   int64
	Error        string
	ErrorCode    string
	Stage        string
	Message      string
}

type createD1ImportNotificationRequest struct {
	BranchName  string `json:"branch_name,omitempty"`
	MigrationID string `json:"migration_id"`
	Event       string `json:"event"`
	Method      string `json:"method,omitempty"`
	ExportBytes int64  `json:"export_bytes,omitempty"`
	TableCount  int    `json:"table_count,omitempty"`
	Matched     *bool  `json:"matched,omitempty"`
	DurationMs  int64  `json:"duration_ms,omitempty"`
	Error       string `json:"error,omitempty"`
	ErrorCode   string `json:"error_code,omitempty"`
	Stage       string `json:"stage,omitempty"`
	Message     string `json:"message,omitempty"`
}

type d1ImportNotificationsService struct {
	client *Client
}

func (s *d1ImportNotificationsService) Create(ctx context.Context, req *CreateD1ImportNotificationRequest) error {
	if req == nil {
		return fmt.Errorf("nil CreateD1ImportNotificationRequest")
	}
	if req.Organization == "" || req.Database == "" {
		return fmt.Errorf("organization and database are required")
	}
	if req.MigrationID == "" || req.Event == "" {
		return fmt.Errorf("migration_id and event are required")
	}

	body := createD1ImportNotificationRequest{
		BranchName:  req.BranchName,
		MigrationID: req.MigrationID,
		Event:       req.Event,
		Method:      req.Method,
		ExportBytes: req.ExportBytes,
		TableCount:  req.TableCount,
		Matched:     req.Matched,
		DurationMs:  req.DurationMs,
		Error:       req.Error,
		ErrorCode:   req.ErrorCode,
		Stage:       req.Stage,
		Message:     req.Message,
	}

	p := path.Join("internal/organizations", req.Organization, "databases", req.Database, "d1-import-notifications")
	httpReq, err := s.client.newRequest(http.MethodPost, p, body)
	if err != nil {
		return err
	}

	return s.client.do(ctx, httpReq, nil)
}

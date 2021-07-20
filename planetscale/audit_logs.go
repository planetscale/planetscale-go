package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

var _ AuditLogsService = &auditlogsService{}

// AuditLogsService is an interface for communicating with the PlanetScale
// AuditLogs API endpoints.
type AuditLogsService interface {
	List(context.Context, *ListAuditLogsRequest) ([]*AuditLog, error)
}

// ListAuditLogsRequest encapsulates the request for listing the audit logs of
// an organization.
type ListAuditLogsRequest struct {
	Organization string
}

// AuditLog represents a PlanetScale audit log.
type AuditLog struct {
	ID   string `json:"id"`
	Type string `json:"type"`

	ActorID          string `json:"actor_id"`
	ActorType        string `json:"actor_type"`
	ActorDisplayName string `json:"actor_display_name"`

	AuditableID          string `json:"auditable_id"`
	AuditableType        string `json:"auditable_type"`
	AuditableDisplayName string `json:"auditable_display_name"`

	AuditAction string `json:"audit_action"`
	Action      string `json:"action"`

	Location string `json:"location"`
	RemoteIP string `json:"remote_ip"`

	TargetID          string `json:"target_id"`
	TargetType        string `json:"target_type"`
	TargetDisplayName string `json:"target_display_name"`

	Metadata map[string]string `json:"metadata"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type auditlogsResponse struct {
	AuditLogs []*AuditLog `json:"data"`
}

type auditlogsService struct {
	client *Client
}

func NewAuditLogsService(client *Client) *auditlogsService {
	return &auditlogsService{
		client: client,
	}
}

// List returns the audit logs for an organization.
func (o *auditlogsService) List(ctx context.Context, listReq *ListAuditLogsRequest) ([]*AuditLog, error) {
	req, err := o.client.newRequest(http.MethodGet, auditlogsAPIPath(listReq.Organization), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for listing audit logs")
	}

	resp := &auditlogsResponse{}
	if err := o.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	return resp.AuditLogs, nil
}

func auditlogsAPIPath(org string) string {
	return fmt.Sprintf("%s/%s/audit-log", organizationsAPIPath, org)
}

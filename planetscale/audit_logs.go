package planetscale

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"path"
	"time"
)

// AuditLogEvent represents an audit log's event type
type AuditLogEvent string

const (
	AuditLogEventBranchCreated                 AuditLogEvent = "branch.created"
	AuditLogEventBranchDeleted                 AuditLogEvent = "branch.deleted"
	AuditLogEventDatabaseCreated               AuditLogEvent = "database.created"
	AuditLogEventDatabaseDeleted               AuditLogEvent = "database.deleted"
	AuditLogEventDeployRequestApproved         AuditLogEvent = "deploy_request.approved"
	AuditLogEventDeployRequestClosed           AuditLogEvent = "deploy_request.closed"
	AuditLogEventDeployRequestCreated          AuditLogEvent = "deploy_request.created"
	AuditLogEventDeployRequestDeleted          AuditLogEvent = "deploy_request.deleted"
	AuditLogEventDeployRequestQueued           AuditLogEvent = "deploy_request.queued"
	AuditLogEventDeployRequestUnqueued         AuditLogEvent = "deploy_request.unqueued"
	AuditLogEventIntegrationCreated            AuditLogEvent = "integration.created"
	AuditLogEventIntegrationDeleted            AuditLogEvent = "integration.deleted"
	AuditLogEventOrganizationInvitationCreated AuditLogEvent = "organization_invitation.created"
	AuditLogEventOrganizationInvitationDeleted AuditLogEvent = "organization_invitation.deleted"
	AuditLogEventOrganizationMembershipCreated AuditLogEvent = "organization_membership.created"
	AuditLogEventOrganizationJoined            AuditLogEvent = "organization.joined"
	AuditLogEventOrganizationRemovedMember     AuditLogEvent = "organization.removed_member"
	AuditLogEventOrganizationDisabledSSO       AuditLogEvent = "organization.disabled_sso"
	AuditLogEventOrganizationEnabledSSO        AuditLogEvent = "organization.enabled_sso"
	AuditLogEventOrganizationUpdatedRole       AuditLogEvent = "organization.updated_role"
	AuditLogEventServiceTokenCreated           AuditLogEvent = "service_token.created"
	AuditLogEventServiceTokenDeleted           AuditLogEvent = "service_token.deleted"
	AuditLogEventServiceTokenGrantedAccess     AuditLogEvent = "service_token.granted_access"
)

var _ AuditLogsService = &auditlogsService{}

// AuditLogsService is an interface for communicating with the PlanetScale
// AuditLogs API endpoints.
type AuditLogsService interface {
	List(context.Context, *ListAuditLogsRequest, ...ListOption) (*CursorPaginatedResponse[*AuditLog], error)
}

// ListAuditLogsRequest encapsulates the request for listing the audit logs of
// an organization.
type ListAuditLogsRequest struct {
	Organization string

	// Events can be used to filter out only the given audit log events.
	Events []AuditLogEvent
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

	Metadata map[string]interface{} `json:"metadata"`

	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
}

type auditlogsService struct {
	client *Client
}

func NewAuditLogsService(client *Client) *auditlogsService {
	return &auditlogsService{
		client: client,
	}
}

// WithEventFilters sets filters on a set of list filters from audit log events.
// For example, `audit_action:database.created`,
// `audit_action:database.deleted`, etc.
func WithEventFilters(events []AuditLogEvent) ListOption {
	return func(opt *ListOptions) error {
		values := opt.URLValues
		if len(events) != 0 {
			for _, action := range events {
				values.Add("filters[]", fmt.Sprintf("audit_action:%s", action))
			}
		}
		return nil
	}
}

// List returns the audit logs for an organization.
func (o *auditlogsService) List(ctx context.Context, listReq *ListAuditLogsRequest, opts ...ListOption) (*CursorPaginatedResponse[*AuditLog], error) {
	if listReq.Organization == "" {
		return nil, errors.New("organization is not set")
	}

	path := auditlogsAPIPath(listReq.Organization)

	defaultOpts := defaultListOptions(WithEventFilters(listReq.Events))
	for _, opt := range opts {
		err := opt(defaultOpts)
		if err != nil {
			return nil, err
		}
	}

	if vals := defaultOpts.URLValues.Encode(); vals != "" {
		path += "?" + vals
	}

	req, err := o.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for listing audit logs: %w", err)
	}

	resp := &CursorPaginatedResponse[*AuditLog]{}
	if err := o.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func auditlogsAPIPath(org string) string {
	return path.Join(organizationsAPIPath, org, "audit-log")
}

package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestAuditLogs_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
  "type": "list",
  "next_page": "https://api.planetscale.com/v1/organizations/planetscale/audit-log?page=2",
  "prev_page": null,
  "data": [
    {
      "id": "ecxuvovgfo95",
      "type": "AuditLogEvent",
      "actor_id": "d4hkujnkswjk",
      "actor_type": "User",
      "auditable_id": "kbog8qlq6lp4",
      "auditable_type": "DeployRequest",
      "target_id": "m40xz7x6gvvk",
      "target_type": "Database",
      "location": "Chicago, IL",
      "target_display_name": "planetscale",
      "metadata": {
        "from": "add-name-to-service-tokens",
        "into": "main"
      },
      "audit_action": "deploy_request.queued",
      "action": "queued",
      "actor_display_name": "Elom Gomez",
      "auditable_display_name": "deploy request #102",
      "remote_ip": "45.19.24.124",
      "created_at": "2021-07-19T17:13:45.000Z",
      "updated_at": "2021-07-19T17:13:45.000Z"
    }
  ]
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	auditLogs, err := client.AuditLogs.List(ctx, &ListAuditLogsRequest{
		Organization: testOrg,
		Events: []AuditLogEvent{
			AuditLogEventBranchDeleted,
			AuditLogEventOrganizationJoined,
		},
	})

	want := []*AuditLog{
		{
			ID:                "ecxuvovgfo95",
			Type:              "AuditLogEvent",
			ActorID:           "d4hkujnkswjk",
			ActorType:         "User",
			AuditableID:       "kbog8qlq6lp4",
			AuditableType:     "DeployRequest",
			TargetID:          "m40xz7x6gvvk",
			TargetType:        "Database",
			Location:          "Chicago, IL",
			TargetDisplayName: "planetscale",
			Metadata: map[string]string{
				"from": "add-name-to-service-tokens",
				"into": "main",
			},
			AuditAction:          "deploy_request.queued",
			Action:               "queued",
			ActorDisplayName:     "Elom Gomez",
			AuditableDisplayName: "deploy request #102",
			RemoteIP:             "45.19.24.124",
			CreatedAt:            time.Date(2021, time.July, 19, 17, 13, 45, 000, time.UTC),
			UpdatedAt:            time.Date(2021, time.July, 19, 17, 13, 45, 000, time.UTC),
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(auditLogs, qt.DeepEquals, want)
}

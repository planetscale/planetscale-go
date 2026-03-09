package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestVtctldOperations_Get(t *testing.T) {
	c := qt.New(t)

	createdAt := time.Date(2026, time.March, 6, 12, 0, 0, 0, time.UTC)
	completedAt := createdAt.Add(2 * time.Minute)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/operations/op-123")

		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`{
			"id":"op-123",
			"type":"VtctldOperation",
			"action":"move_tables_switch_traffic",
			"timeout":300,
			"created_at":"2026-03-06T12:00:00Z",
			"completed_at":"2026-03-06T12:02:00Z",
			"state":"completed",
			"completed":true,
			"metadata":{"workflow":"migrate_commerce","target_keyspace":"commerce"},
			"result":{"summary":"done"},
			"error":""
		}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	operation, err := client.Vtctld.GetOperation(context.Background(), &GetVtctldOperationRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		ID:           "op-123",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(operation.ID, qt.Equals, "op-123")
	c.Assert(operation.Action, qt.Equals, "move_tables_switch_traffic")
	c.Assert(operation.State, qt.Equals, "completed")
	c.Assert(operation.Completed, qt.IsTrue)
	c.Assert(operation.Timeout, qt.Equals, 300)
	c.Assert(operation.CreatedAt, qt.Equals, createdAt)
	c.Assert(*operation.CompletedAt, qt.Equals, completedAt)
	c.Assert(string(operation.Metadata), qt.JSONEquals, map[string]interface{}{
		"workflow":        "migrate_commerce",
		"target_keyspace": "commerce",
	})
	c.Assert(string(operation.Result), qt.Equals, `{"summary":"done"}`)
	c.Assert(operation.Error, qt.Equals, "")
}

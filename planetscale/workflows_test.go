package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestWorkflows_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"type":"list","current_page":1,"next_page":null,"next_page_url":null,"prev_page":null,"prev_page_url":null,"data":[{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"completed","created_at":"2025-03-18T16:22:15.293Z","updated_at":"2025-03-18T16:22:15.293Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"ddi0rgmj636p","type":"Branch","name":"main","created_at":"2025-03-18T16:22:14.872Z","deleted_at":null,"updated_at":"2025-03-18T16:22:15.169Z"},"source_keyspace":{"id":"ki6zinvzi973","type":"BranchKeyspace","name":"green-morning-8381","created_at":"2025-03-18T16:22:15.016Z","deleted_at":null,"updated_at":"2025-03-18T16:22:15.128Z"},"target_keyspace":{"id":"n4bqtq0akviv","type":"BranchKeyspace","name":"lively-sun-1587","created_at":"2025-03-18T16:22:15.240Z","deleted_at":null,"updated_at":"2025-03-18T16:22:15.240Z"},"actor":{"id":"lcuyaidzbteb","type":"User","display_name":"kyudcz4fng@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflows, err := client.Workflows.List(ctx, &ListWorkflowsRequest{
		Organization: "foo",
		Database:     "bar",
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(len(workflows), qt.Equals, 1)
	c.Assert(workflows[0].ID, qt.Equals, wantID)
	c.Assert(workflows[0].Name, qt.Equals, "shard-table")
	c.Assert(workflows[0].Number, qt.Equals, 1)
}

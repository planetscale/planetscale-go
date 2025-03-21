package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestWorkflows_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"type":"list","current_page":1,"next_page":null,"next_page_url":null,"prev_page":null,"prev_page_url":null,"data":[{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"completed","created_at":"2025-03-18T16:22:15.293Z","updated_at":"2025-03-18T16:22:15.293Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"ddi0rgmj636p","type":"Branch","name":"main","created_at":"2025-03-18T16:22:14.872Z","deleted_at":null,"updated_at":"2025-03-18T16:22:15.169Z"},"source_keyspace":{"id":"ki6zinvzi973","type":"BranchKeyspace","name":"source-keyspace","created_at":"2025-03-18T16:22:15.016Z","deleted_at":null,"updated_at":"2025-03-18T16:22:15.128Z"},"target_keyspace":{"id":"n4bqtq0akviv","type":"BranchKeyspace","name":"target-keyspace","created_at":"2025-03-18T16:22:15.240Z","deleted_at":null,"updated_at":"2025-03-18T16:22:15.240Z"},"actor":{"id":"lcuyaidzbteb","type":"User","display_name":"kyudcz4fng@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows")
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
	c.Assert(workflows[0].Number, qt.Equals, uint64(1))
}

func TestWorkflows_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"pending","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.Get(ctx, &GetWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(workflow.VDiff.State, qt.Equals, "pending")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"pending","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"table-1","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10},{"name":"table-2"}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.Create(ctx, &CreateWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		Branch:         "branch",
		Name:           "my-workflow",
		SourceKeyspace: "source-keyspace",
		TargetKeyspace: "target-keyspace",
		Tables:         []string{"table1", "table2"},
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(workflow.VDiff.State, qt.Equals, "pending")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 2)
}

func TestWorkflows_CreateWithNewOptions(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"pending","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"table-1","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10},{"name":"table-2"}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		var request struct {
			GlobalKeyspace     *string `json:"global_keyspace"`
			DeferSecondaryKeys *bool   `json:"defer_secondary_keys"`
			OnDDL              *string `json:"on_ddl"`
		}
		err = json.NewDecoder(r.Body).Decode(&request)
		c.Assert(err, qt.IsNil)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows")
		c.Assert(*request.GlobalKeyspace, qt.Equals, "baz")
		c.Assert(*request.DeferSecondaryKeys, qt.Equals, true)
		c.Assert(*request.OnDDL, qt.Equals, "EXEC_IGNORE")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	onDDL := "EXEC_IGNORE"
	globalKeyspace := "baz"
	deferSecondaryKeys := true

	workflow, err := client.Workflows.Create(ctx, &CreateWorkflowRequest{
		Organization:       "foo",
		Database:           "bar",
		Branch:             "branch",
		Name:               "my-workflow",
		SourceKeyspace:     "source-keyspace",
		TargetKeyspace:     "target-keyspace",
		Tables:             []string{"table1", "table2"},
		OnDDL:              &onDDL,
		GlobalKeyspace:     &globalKeyspace,
		DeferSecondaryKeys: &deferSecondaryKeys,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(workflow.VDiff.State, qt.Equals, "pending")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 2)
}

func TestWorfklows_VerifyData(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"pending","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"table-1","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10},{"name":"table-2"}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/verify-data")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.VerifyData(ctx, &VerifyDataWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(workflow.VDiff.State, qt.Equals, "pending")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 2)
}

func TestWorkflows_SwitchReplicas(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"switched_replicas","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/switch-replicas")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.SwitchReplicas(ctx, &SwitchReplicasWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "switched_replicas")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_SwitchPrimaries(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"switched_primaries","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/switch-primaries")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.SwitchPrimaries(ctx, &SwitchPrimariesWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "switched_primaries")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_ReverseTraffic(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"running","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/reverse-traffic")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.ReverseTraffic(ctx, &ReverseTrafficWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "running")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_Cutover(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"cutover","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/cutover")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.Cutover(ctx, &CutoverWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "cutover")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_ReverseCutover(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"reversed_cutover","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/reverse-cutover")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.ReverseCutover(ctx, &ReverseCutoverWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "reversed_cutover")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_Complete(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"complete","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/complete")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.Complete(ctx, &CompleteWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "complete")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_Retry(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"running","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1/retry")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.Retry(ctx, &RetryWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "running")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

func TestWorkflows_Cancel(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"thisisanid","type":"Workflow","name":"shard-table","number":1,"state":"cancelled","created_at":"2025-03-18T17:21:55.546Z","updated_at":"2025-03-18T17:21:55.618Z","started_at":null,"completed_at":null,"cancelled_at":null,"reversed_at":null,"retried_at":null,"data_copy_completed_at":null,"cutover_at":null,"replicas_switched":false,"primaries_switched":false,"switch_replicas_at":null,"switch_primaries_at":null,"verify_data_at":null,"workflow_type":"move_tables","workflow_subtype":null,"may_retry":false,"verified_data_stale":false,"branch":{"id":"hjcq437nimp2","type":"Branch","name":"branch","created_at":"2025-03-18T17:21:55.194Z","updated_at":"2025-03-18T17:21:55.434Z","restore_checklist_completed_at":null,"schema_last_updated_at":"2025-03-18T17:21:55.360Z","mysql_address":"us-east.connect.psdb.cloud","mysql_provider_address":"aws.connect.psdb.cloud","schema_ready":true,"state":"ready","vtgate_size":"vg.c1.nano","vtgate_count":1,"cluster_rate_name":"PS_10","mysql_edge_address":"aws.connect.psdb.cloud","ready":true,"metal":false,"production":true,"safe_migrations":true,"sharded":true,"shard_count":4,"stale_schema":false,"index_usage_enabled":true,"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"restored_from_branch":null,"private_connectivity":false,"private_edge_connectivity":false,"html_url":"http://app.pscaledev.com:3001/organization1/weathered-bush-4453/main","has_replicas":true,"has_read_only_replicas":false,"url":"http://api.pscaledev.com:3000/v1/organizations/organization1/databases/weathered-bush-4453/branches/main","region":{"id":"ps-region-id","type":"Region","provider":"AWS","enabled":true,"public_ip_addresses":[],"display_name":"AWS us-east-1","location":"Ashburn, Virginia","slug":"us-east","current_default":true},"parent_branch":null},"source_keyspace":{"id":"w7l4fekda4xg","type":"BranchKeyspace","name":"source-keyspace","shards":1,"sharded":false,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.284Z","updated_at":"2025-03-18T17:21:55.390Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"target_keyspace":{"id":"65qnxzwehl6f","type":"BranchKeyspace","name":"target-keyspace","shards":4,"sharded":true,"replicas":2,"extra_replicas":0,"created_at":"2025-03-18T17:21:55.499Z","updated_at":"2025-03-18T17:21:55.499Z","cluster_rate_name":"PS_10","cluster_rate_display_name":"PS-10","resizing":false,"ready":true,"metal":false,"vector_pool_allocation":null,"resize_pending":false},"actor":{"id":"55cloymikacf","type":"User","display_name":"wabz2zww54@pscaledev.com","avatar_url":"https://app.planetscale.com/gravatar-fallback.png"},"verify_data_by":null,"reversed_by":null,"switch_replicas_by":null,"switch_primaries_by":null,"cancelled_by":null,"completed_by":null,"retried_by":null,"cutover_by":null,"reversed_cutover_by":null,"streams":[{"id":"z7orf7caq72o","type":"WorkflowStream","state":"copying","created_at":"2025-03-18T17:21:55.598Z","updated_at":"2025-03-18T17:21:55.598Z","vitess_id":1,"target_shard":"-80","source_shard":"-80","target_tablet_uid":"target-uid","target_tablet_cell":"target-cell","position":"position","stop_position":"stop-position","rows_copied":10,"component_throttled":null,"component_throttled_at":null,"primary_serving":false,"info":"important info"}],"tables":[{"id":"5lje0cf2dvdi","type":"WorkflowTable","name":"cool-snowflake-2076","created_at":"2025-03-18T17:21:55.584Z","updated_at":"2025-03-18T17:21:55.584Z","rows_copied":10,"rows_total":100,"rows_percentage":10}],"vdiff":{"id":"n86yn5nr26zc","type":"WorkflowVDiff","state":"pending","created_at":"2025-03-18T17:21:55.614Z","updated_at":"2025-03-18T17:21:55.614Z","started_at":null,"completed_at":null,"has_mismatch":false,"progress_percentage":null,"eta_seconds":null,"table_reports":[{"id":"rw5saem6ltq0","type":"WorkflowVDiffTableReport","table_name":"users","shard":"-","mismatched_rows_count":0,"extra_source_rows_count":0,"extra_target_rows_count":0,"extra_source_rows":[],"extra_target_rows":[],"mismatched_rows":[],"sample_extra_source_rows_query":null,"sample_extra_target_rows_query":null,"sample_mismatched_rows_query":null,"created_at":"2025-03-18T17:21:55.624Z","updated_at":"2025-03-18T17:21:55.624Z"}]}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodDelete)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/foo/databases/bar/workflows/1")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	workflow, err := client.Workflows.Cancel(ctx, &CancelWorkflowRequest{
		Organization:   "foo",
		Database:       "bar",
		WorkflowNumber: 1,
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(workflow.ID, qt.Equals, wantID)
	c.Assert(workflow.Name, qt.Equals, "shard-table")
	c.Assert(workflow.Number, qt.Equals, uint64(1))
	c.Assert(workflow.State, qt.Equals, "cancelled")
	c.Assert(workflow.SourceKeyspace.Name, qt.Equals, "source-keyspace")
	c.Assert(workflow.TargetKeyspace.Name, qt.Equals, "target-keyspace")
	c.Assert(workflow.Branch.Name, qt.Equals, "branch")
	c.Assert(len(workflow.Streams), qt.Equals, 1)
	c.Assert(len(workflow.Tables), qt.Equals, 1)
}

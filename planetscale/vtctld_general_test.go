package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestVtctld_ListWorkflows(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/workflows")
		c.Assert(r.URL.Query().Get("keyspace"), qt.Equals, "my-keyspace")
		c.Assert(r.URL.Query().Get("workflow"), qt.Equals, "my-workflow")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Vtctld.ListWorkflows(ctx, &VtctldListWorkflowsRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Keyspace:     "my-keyspace",
		Workflow:     "my-workflow",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVtctld_ListWorkflows_NoWorkflowFilter(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/workflows")
		c.Assert(r.URL.Query().Get("keyspace"), qt.Equals, "my-keyspace")
		c.Assert(r.URL.Query().Get("workflow"), qt.Equals, "")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Vtctld.ListWorkflows(ctx, &VtctldListWorkflowsRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Keyspace:     "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVtctld_ListWorkflows_IncludeLogs(t *testing.T) {
	tests := []struct {
		name               string
		includeLogs        *bool
		expectIncludeParam bool
		expectedInclude    string
	}{
		{name: "include_logs false", includeLogs: boolPtr(false), expectIncludeParam: true, expectedInclude: "false"},
		{name: "include_logs omitted", includeLogs: nil, expectIncludeParam: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := qt.New(t)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c.Assert(r.Method, qt.Equals, http.MethodGet)
				c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/workflows")
				c.Assert(r.URL.Query().Get("keyspace"), qt.Equals, "my-keyspace")

				_, hasIncludeLogs := r.URL.Query()["include_logs"]
				if tt.expectIncludeParam {
					c.Assert(hasIncludeLogs, qt.IsTrue)
					c.Assert(r.URL.Query().Get("include_logs"), qt.Equals, tt.expectedInclude)
				} else {
					c.Assert(hasIncludeLogs, qt.IsFalse)
				}

				w.WriteHeader(200)
				_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
				c.Assert(err, qt.IsNil)
			}))
			defer ts.Close()

			client, err := NewClient(WithBaseURL(ts.URL))
			c.Assert(err, qt.IsNil)

			ctx := context.Background()
			data, err := client.Vtctld.ListWorkflows(ctx, &VtctldListWorkflowsRequest{
				Organization: "my-org",
				Database:     "my-db",
				Branch:       "my-branch",
				Keyspace:     "my-keyspace",
				IncludeLogs:  tt.includeLogs,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
		})
	}
}

func TestVtctld_ListKeyspaces(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/keyspaces")
		c.Assert(r.URL.Query().Get("name"), qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Vtctld.ListKeyspaces(ctx, &VtctldListKeyspacesRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Name:         "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVtctld_StartWorkflow(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/workflows/my-workflow/start")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"summary":"started"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Vtctld.StartWorkflow(ctx, &VtctldStartWorkflowRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Workflow:     "my-workflow",
		Keyspace:     "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"summary":"started"}`)
}

func TestVtctld_StopWorkflow(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/workflows/my-workflow/stop")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"summary":"stopped"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Vtctld.StopWorkflow(ctx, &VtctldStopWorkflowRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Workflow:     "my-workflow",
		Keyspace:     "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"summary":"stopped"}`)
}

func TestVtctld_ListKeyspaces_NoNameFilter(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vtctld/keyspaces")
		c.Assert(r.URL.Query().Get("name"), qt.Equals, "")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Vtctld.ListKeyspaces(ctx, &VtctldListKeyspacesRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

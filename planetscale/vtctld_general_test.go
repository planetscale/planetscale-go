package planetscale

import (
	"context"
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

package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMoveTables_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["workflow"], qt.Equals, "my-workflow")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"summary":"created"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.Create(ctx, &MoveTablesCreateRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
		SourceKeyspace: "source",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"summary":"created"}`)
}

func TestMoveTables_Show(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows/my-workflow")
		c.Assert(r.URL.Query().Get("target_keyspace"), qt.Equals, "target")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.Show(ctx, &MoveTablesShowRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMoveTables_Status(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows/my-workflow/status")
		c.Assert(r.URL.Query().Get("target_keyspace"), qt.Equals, "target")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.Status(ctx, &MoveTablesStatusRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMoveTables_SwitchTraffic(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows/my-workflow/switch-traffic")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.SwitchTraffic(ctx, &MoveTablesSwitchTrafficRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMoveTables_ReverseTraffic(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows/my-workflow/reverse-traffic")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.ReverseTraffic(ctx, &MoveTablesReverseTrafficRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMoveTables_Cancel(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows/my-workflow/cancel")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.Cancel(ctx, &MoveTablesCancelRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMoveTables_Complete(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/move-tables/workflows/my-workflow/complete")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.MoveTables.Complete(ctx, &MoveTablesCompleteRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

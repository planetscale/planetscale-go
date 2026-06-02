package planetscale

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestProcesslist_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/processlist")
		c.Assert(r.URL.Query().Get("keyspace"), qt.Equals, "commerce")
		c.Assert(r.URL.Query().Get("shard"), qt.Equals, "-80")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"keyspace":"commerce","shard":"-80","tablet":"zone1-1001","processes":[{"id":101,"user":"vt_app","command":"Query","time":42,"info":"SELECT 1"}]}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	result, err := client.Processlist.List(ctx, &ProcesslistRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Keyspace:     "commerce",
		Shard:        "-80",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Tablet, qt.Equals, "zone1-1001")
	c.Assert(result.Processes, qt.HasLen, 1)
	c.Assert(result.Processes[0].ID, qt.Equals, int64(101))
	c.Assert(result.Processes[0].User, qt.Equals, "vt_app")
}

func TestProcesslist_List_OmitsEmptyParams(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Query().Has("keyspace"), qt.IsFalse)
		c.Assert(r.URL.Query().Has("shard"), qt.IsFalse)

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"keyspace":"main","shard":"-","tablet":"zone1-2001","processes":[]}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	result, err := client.Processlist.List(ctx, &ProcesslistRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Keyspace, qt.Equals, "main")
}

func TestProcesslist_Kill(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/processlist/kill")

		body, err := io.ReadAll(r.Body)
		c.Assert(err, qt.IsNil)
		var payload map[string]any
		c.Assert(json.Unmarshal(body, &payload), qt.IsNil)
		c.Assert(payload["id"], qt.Equals, float64(101))
		c.Assert(payload["kind"], qt.Equals, "query")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"success":true,"keyspace":"main","shard":"-","tablet":"zone1-2001","id":101,"kind":"query"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	result, err := client.Processlist.Kill(ctx, &KillProcessRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		ID:           101,
		Kind:         "query",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(result.Success, qt.IsTrue)
	c.Assert(result.ID, qt.Equals, int64(101))
	c.Assert(result.Kind, qt.Equals, "query")
}

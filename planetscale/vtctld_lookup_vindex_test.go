package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestLookupVindex_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/lookup-vindex/vindexes")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["name"], qt.Equals, "my-vindex")
		c.Assert(body["table_keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.LookupVindex.Create(ctx, &LookupVindexCreateRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		Name:          "my-vindex",
		TableKeyspace: "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestLookupVindex_Show(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/lookup-vindex/vindexes/my-vindex")
		c.Assert(r.URL.Query().Get("table_keyspace"), qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.LookupVindex.Show(ctx, &LookupVindexShowRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		Name:          "my-vindex",
		TableKeyspace: "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestLookupVindex_Externalize(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/lookup-vindex/vindexes/my-vindex/externalize")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["table_keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.LookupVindex.Externalize(ctx, &LookupVindexExternalizeRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		Name:          "my-vindex",
		TableKeyspace: "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestLookupVindex_Internalize(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/lookup-vindex/vindexes/my-vindex/internalize")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["table_keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.LookupVindex.Internalize(ctx, &LookupVindexInternalizeRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		Name:          "my-vindex",
		TableKeyspace: "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestLookupVindex_Cancel(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/lookup-vindex/vindexes/my-vindex/cancel")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["table_keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.LookupVindex.Cancel(ctx, &LookupVindexCancelRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		Name:          "my-vindex",
		TableKeyspace: "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestLookupVindex_Complete(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/lookup-vindex/vindexes/my-vindex/complete")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["table_keyspace"], qt.Equals, "my-keyspace")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.LookupVindex.Complete(ctx, &LookupVindexCompleteRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		Name:          "my-vindex",
		TableKeyspace: "my-keyspace",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

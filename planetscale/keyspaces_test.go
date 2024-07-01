package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestKeyspaces_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"type":"list","current_page":1,"next_page":null,"next_page_url":null,"prev_page":null,"prev_page_url":null,"data":[{"id":"thisisanid","type":"Keyspace","name":"planetscale","shards":2,"sharded":true,"created_at":"2022-01-14T15:39:28.394Z","updated_at":"2021-12-20T21:11:07.697Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	keyspaces, err := client.Keyspaces.List(ctx, &ListBranchKeyspacesRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(len(keyspaces), qt.Equals, 1)
	c.Assert(keyspaces[0].ID, qt.Equals, wantID)
	c.Assert(keyspaces[0].Sharded, qt.Equals, true)
	c.Assert(keyspaces[0].Shards, qt.Equals, 2)
}

func TestKeyspaces_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"type":"Keyspace","id":"thisisanid","name":"planetscale","shards":2,"sharded":true,"created_at":"2022-01-14T15:39:28.394Z","updated_at":"2021-12-20T21:11:07.697Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	keyspace, err := client.Keyspaces.Get(ctx, &GetBranchKeyspaceRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
		Keyspace:     "qux",
	})

	wantID := "thisisanid"

	c.Assert(err, qt.IsNil)
	c.Assert(keyspace.ID, qt.Equals, wantID)
	c.Assert(keyspace.Sharded, qt.Equals, true)
	c.Assert(keyspace.Shards, qt.Equals, 2)
}

func TestKeyspaces_VSchema(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"raw":"{\"sharded\":true,\"tables\":{}}","html":"<div>\"sharded\":true,\"tables\":{}</div>"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	vSchema, err := client.Keyspaces.VSchema(ctx, &GetKeyspaceVSchemaRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
		Keyspace:     "qux",
	})

	wantRaw := "{\"sharded\":true,\"tables\":{}}"
	wantHTML := "<div>\"sharded\":true,\"tables\":{}</div>"

	c.Assert(err, qt.IsNil)
	c.Assert(vSchema.Raw, qt.Equals, wantRaw)
	c.Assert(vSchema.HTML, qt.Equals, wantHTML)
}

func TestKeyspaces_UpdateVSchema(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"raw":"{\"sharded\":true,\"tables\":{}}","html":"<div>\"sharded\":true,\"tables\":{}</div>"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.Method, qt.Equals, http.MethodPatch)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	vSchema, err := client.Keyspaces.UpdateVSchema(ctx, &UpdateKeyspaceVSchemaRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
		Keyspace:     "qux",
		VSchema:      "{\"sharded\":true,\"tables\":{}}",
	})

	wantRaw := "{\"sharded\":true,\"tables\":{}}"
	wantHTML := "<div>\"sharded\":true,\"tables\":{}</div>"

	c.Assert(err, qt.IsNil)
	c.Assert(vSchema.Raw, qt.Equals, wantRaw)
	c.Assert(vSchema.HTML, qt.Equals, wantHTML)
}

package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

const testPostgresBranch = "postgres-test-branch"

func TestPostgresBranches_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"postgres-test-branch","name":"postgres-test-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z", "region": {"slug": "us-west", "display_name": "US West"}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "postgres-test-db"

	branch, err := client.PostgresBranches.Create(ctx, &CreatePostgresBranchRequest{
		Organization: org,
		Database:     name,
		Region:       "us-west",
		Name:         testPostgresBranch,
		ParentBranch: "main",
	})

	want := &PostgresBranch{
		ID:   "postgres-test-branch",
		Name: testPostgresBranch,
		Region: Region{
			Slug: "us-west",
			Name: "US West",
		},
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(branch, qt.DeepEquals, want)
}

func TestPostgresBranches_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"id":"postgres-test-branch","name":"postgres-test-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "postgres-test-db"

	branches, err := client.PostgresBranches.List(ctx, &ListPostgresBranchesRequest{
		Organization: org,
		Database:     name,
	})

	want := []*PostgresBranch{{
		ID:        "postgres-test-branch",
		Name:      testPostgresBranch,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}}

	c.Assert(err, qt.IsNil)
	c.Assert(branches, qt.DeepEquals, want)
}

func TestPostgresBranches_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"postgres-test-branch","name":"postgres-test-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "postgres-test-db"

	branch, err := client.PostgresBranches.Get(ctx, &GetPostgresBranchRequest{
		Organization: org,
		Database:     name,
		Branch:       testPostgresBranch,
	})

	want := &PostgresBranch{
		ID:        "postgres-test-branch",
		Name:      testPostgresBranch,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(branch, qt.DeepEquals, want)
}

func TestPostgresBranches_Schema(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"name": "test_schema", "raw": "CREATE TABLE test...", "html": "<div>CREATE TABLE test...</div>"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	schema, err := client.PostgresBranches.Schema(ctx, &PostgresBranchSchemaRequest{
		Organization: "my-org",
		Database:     "postgres-test-db",
		Branch:       testPostgresBranch,
	})

	want := &PostgresBranchSchema{
		Name: "test_schema",
		Raw:  "CREATE TABLE test...",
		HTML: "<div>CREATE TABLE test...</div>",
	}

	c.Assert(err, qt.IsNil)
	c.Assert(schema, qt.DeepEquals, want)
}

func TestPostgresBranches_ListClusterSKUs(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `[
		{
			"name": "PS_10",
			"type": "ClusterSizeSku",
			"display_name": "PS-10",
			"cpu": "1/8",
			"provider_instance_type": null,
			"storage": null,
			"ram": 1,
			"enabled": true,
			"provider": null,
			"rate": null,
			"replica_rate": null,
			"default_vtgate": "VTG_5",
			"default_vtgate_rate": null,
			"sort_order": 1
		}
	]`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	skus, err := client.PostgresBranches.ListClusterSKUs(ctx, &ListBranchClusterSKUsRequest{
		Organization: "my-org",
		Database:     "postgres-test-db",
		Branch:       testPostgresBranch,
	})

	want := []*ClusterSKU{
		{
			Name:          "PS_10",
			DisplayName:   "PS-10",
			CPU:           "1/8",
			Memory:        1,
			Enabled:       true,
			DefaultVTGate: "VTG_5",
			SortOrder:     1,
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(skus, qt.DeepEquals, want)
}

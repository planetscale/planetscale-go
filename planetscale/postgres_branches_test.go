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
		out := `{"data": [{"name": "test_schema", "raw": "CREATE TABLE test...", "html": "<div>CREATE TABLE test...</div>"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	schemas, err := client.PostgresBranches.Schema(ctx, &PostgresBranchSchemaRequest{
		Organization: "my-org",
		Database:     "postgres-test-db",
		Branch:       testPostgresBranch,
	})

	want := []*PostgresBranchSchema{
		{
			Name: "test_schema",
			Raw:  "CREATE TABLE test...",
			HTML: "<div>CREATE TABLE test...</div>",
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(schemas, qt.DeepEquals, want)
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

func TestPostgresBranches_Parameters(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `[
		{
			"type": "PostgresClusterParameter",
			"name": "peers",
			"display_name": "Number of processes",
			"namespace": "psbouncer",
			"category": "PSBouncer",
			"description": "Sets the number of PSBouncer processes that will run on each node in this branch's cluster.\n",
			"extension": false,
			"internal": false,
			"parameter_type": "integer",
			"default_value": "1",
			"value": "1",
			"required": true,
			"created_at": "2025-06-13T15:03:14.578Z",
			"updated_at": "2025-07-08T21:13:37.019Z",
			"restart": true,
			"max": 4,
			"min": 1,
			"url": null,
			"actor": {
				"id": "somepublicid",
				"type": "User",
				"display_name": "Test User",
				"avatar_url": ""
			}
		}
	]`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	parameters, err := client.PostgresBranches.Parameters(ctx, &GetPostgresBranchParametersRequest{
		Organization: "my-org",
		Database:     "postgres-test-db",
		Branch:       testPostgresBranch,
	})

	maxPeers := 4
	minPeers := 1

	want := []*PostgresBranchParameter{
		{
			Type:          "PostgresClusterParameter",
			Name:          "peers",
			DisplayName:   "Number of processes",
			Namespace:     "psbouncer",
			Category:      "PSBouncer",
			Description:   "Sets the number of PSBouncer processes that will run on each node in this branch's cluster.\n",
			Extension:     false,
			Internal:      false,
			ParameterType: "integer",
			DefaultValue:  "1",
			Value:         "1",
			Required:      true,
			CreatedAt:     time.Date(2025, time.June, 13, 15, 3, 14, 578000000, time.UTC),
			UpdatedAt:     time.Date(2025, time.July, 8, 21, 13, 37, 19000000, time.UTC),
			Restart:       true,
			Max:           &maxPeers,
			Min:           &minPeers,
			URL:           nil,
			Actor: Actor{
				ID:   "somepublicid",
				Type: "User",
				Name: "Test User",
			},
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(parameters, qt.DeepEquals, want)
}

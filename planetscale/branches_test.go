package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

const testBranch = "planetscale-go-test-db-branch"

func TestDatabaseBranches_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z", "region": {"slug": "us-west", "display_name": "US West"}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.Create(ctx, &CreateDatabaseBranchRequest{
		Organization: org,
		Database:     name,
		Region:       "us-west",
		Name:         testBranch,
	})

	want := &DatabaseBranch{
		Name: testBranch,
		Region: Region{
			Slug: "us-west",
			Name: "US West",
		},
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabaseBranches_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.List(ctx, &ListDatabaseBranchesRequest{
		Organization: org,
		Database:     name,
	})

	want := []*DatabaseBranch{{
		Name:      testBranch,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabaseBranches_ListEmpty(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.List(ctx, &ListDatabaseBranchesRequest{
		Organization: org,
		Database:     name,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.HasLen, 0)
}

func TestDatabaseBranches_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.Get(ctx, &GetDatabaseBranchRequest{
		Organization: org,
		Database:     name})

	want := &DatabaseBranch{
		Name:      testBranch,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestBranches_Diff(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"name": "foo"}, {"name": "bar"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	diffs, err := client.DatabaseBranches.Diff(ctx, &DiffBranchRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
	})

	want := []*Diff{
		{Name: "foo"},
		{Name: "bar"},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(diffs, qt.DeepEquals, want)
}

func TestBranches_Schema(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"name": "foo"}, {"name": "bar"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	schemas, err := client.DatabaseBranches.Schema(ctx, &BranchSchemaRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
	})

	want := []*Diff{
		{Name: "foo"},
		{Name: "bar"},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(schemas, qt.DeepEquals, want)
}

func TestBranches_VSchema(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"raw": "{}"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	vSchema, err := client.DatabaseBranches.VSchema(ctx, &BranchVSchemaRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
	})

	want := `{}`

	c.Assert(err, qt.IsNil)
	c.Assert(vSchema.Raw, qt.DeepEquals, want)
}

func TestBranches_Keyspaces(t *testing.T) {
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

	keyspaces, err := client.DatabaseBranches.Keyspaces(ctx, &BranchKeyspacesRequest{
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

func TestBranches_RefreshSchema(t *testing.T) {
	c := qt.New(t)

	wantURL := "/v1/organizations/my-org/databases/planetscale-go-test-db/branches/planetscale-go-test-db-branch/refresh-schema"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(201)
		c.Assert(r.URL.String(), qt.DeepEquals, wantURL)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	err = client.DatabaseBranches.RefreshSchema(ctx, &RefreshSchemaRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Branch:       testBranch,
	})
	c.Assert(err, qt.IsNil)
}

func TestBranches_Promote(t *testing.T) {
	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(http.MethodPost, qt.Equals, r.Method)
		w.WriteHeader(200)

		out := `
{
	"id": "test-promotion-branch",
	"type": "BranchPromotionRequest",
	"state": "promoted",
	"created_at": "2021-01-14T10:19:23.000Z",
	"updated_at": "2021-01-14T10:19:23.000Z",
	"started_at": "2021-01-14T10:19:23.000Z",
	"finished_at": "2021-01-14T10:19:23.000Z",
	"lint_errors": null,
	"branch": "main",
		"actor": {
			"id": "test-promotion-branch",
			"type": "User",
			"display_name": "Test User",
			"name": "Test User",
			"nickname": null,
			"email": "test@example.com",
			"avatar_url": "https://www.gravatar.com/avatar/4c97310eb2f0e43f486380f040398a02?d=https%3A%2F%2Fapp.planetscale.com%2Fgravatar-fallback.png&s=64",
			"created_at": "2021-08-25T21:22:20.150Z",
			"updated_at": "2021-08-26T20:08:14.725Z",
			"two_factor_auth_configured": false
		}

}`

		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.Promote(ctx, &PromoteRequest{
		Organization: org,
		Database:     name,
		Branch:       "planetscale-go-test-db-branch",
	})

	want := &BranchPromotionRequest{
		ID:         "test-promotion-branch",
		State:      "promoted",
		Branch:     "main",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		StartedAt:  &testTime,
		FinishedAt: &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestBranches_GetPromotionRequest(t *testing.T) {
	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(http.MethodGet, qt.Equals, r.Method)
		w.WriteHeader(200)

		out := `
{
	"id": "test-promotion-branch",
	"type": "BranchPromotionRequest",
	"state": "promoted",
	"created_at": "2021-01-14T10:19:23.000Z",
	"updated_at": "2021-01-14T10:19:23.000Z",
	"started_at": "2021-01-14T10:19:23.000Z",
	"finished_at": "2021-01-14T10:19:23.000Z",
	"lint_errors": null,
	"branch": "main",
		"actor": {
			"id": "test-promotion-branch",
			"type": "User",
			"display_name": "Test User",
			"name": "Test User",
			"nickname": null,
			"email": "test@example.com",
			"avatar_url": "https://www.gravatar.com/avatar/4c97310eb2f0e43f486380f040398a02?d=https%3A%2F%2Fapp.planetscale.com%2Fgravatar-fallback.png&s=64",
			"created_at": "2021-08-25T21:22:20.150Z",
			"updated_at": "2021-08-26T20:08:14.725Z",
			"two_factor_auth_configured": false
		}
}`

		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.GetPromotionRequest(ctx, &GetPromotionRequestRequest{
		Organization: org,
		Database:     name,
		Branch:       "planetscale-go-test-db-branch",
	})

	want := &BranchPromotionRequest{
		ID:         "test-promotion-branch",
		State:      "promoted",
		Branch:     "main",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		StartedAt:  &testTime,
		FinishedAt: &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestBranches_DemoteWithDemotionRequest(t *testing.T) {
	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(http.MethodPost, qt.Equals, r.Method)
		w.WriteHeader(200)
		out := `
	{
	"id": "test-demotion-request",
	"type": "BranchDemotionRequest",
	"branch": "main",
	"actor": {
	"id": "test-actor",
	"type": "User",
	"display_name": "Test User",
	"name": null,
	"email": "test@example.com",
	"avatar_url": "https://app.planetscale.com/gravatar-fallback.png",
	"created_at": "2021-01-14T10:19:23.000Z",
				"updated_at": "2021-01-14T10:19:23.000Z",
	"two_factor_auth_configured": false
	},
	"responder": null,
	"created_at": "2021-01-14T10:19:23.000Z",
	"responded_at": null,
	"state": "pending"
	}
`

		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "my-test-db"
	branch := "main"

	dr, err := client.DatabaseBranches.Demote(ctx, &DemoteRequest{
		Organization: org,
		Database:     name,
		Branch:       branch,
	})

	want := &BranchDemotionRequest{
		ID:    "test-demotion-request",
		State: "pending",
		Actor: &Actor{
			ID:   "test-actor",
			Type: "User",
			Name: "Test User",
		},
		CreatedAt:   testTime,
		RespondedAt: nil,
		Responder:   nil,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestBranches_DemoteNoDemotionRequest(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(http.MethodPost, qt.Equals, r.Method)
		w.WriteHeader(204)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "my-test-db"
	branch := "main"

	dr, err := client.DatabaseBranches.Demote(ctx, &DemoteRequest{
		Organization: org,
		Database:     name,
		Branch:       branch,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.IsNil)
}

func TestBranches_GetDemotionRequest(t *testing.T) {
	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(http.MethodGet, qt.Equals, r.Method)
		w.WriteHeader(200)
		out := `
{
	"id": "test-demotion-request",
	"type": "BranchDemotionRequest",
	"branch": "main",
	"actor": {
	"id": "test-actor",
	"type": "User",
	"display_name": "Test User",
	"name": null,
	"email": "test@example.com",
	"avatar_url": "https://app.planetscale.com/gravatar-fallback.png",
	"created_at": "2021-01-14T10:19:23.000Z",
				"updated_at": "2021-01-14T10:19:23.000Z",
	"two_factor_auth_configured": false
	},
	"responder": null,
	"created_at": "2021-01-14T10:19:23.000Z",
	"responded_at": null,
	"state": "pending"
	}
`

		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "my-test-db"
	branch := "main"

	dr, err := client.DatabaseBranches.GetDemotionRequest(ctx, &GetDemotionRequestRequest{
		Organization: org,
		Database:     name,
		Branch:       branch,
	})

	want := &BranchDemotionRequest{
		ID:    "test-demotion-request",
		State: "pending",
		Actor: &Actor{
			ID:   "test-actor",
			Type: "User",
			Name: "Test User",
		},
		CreatedAt:   testTime,
		RespondedAt: nil,
		Responder:   nil,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestBranches_DenyDemotionRequest(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(http.MethodDelete, qt.Equals, r.Method)
		w.WriteHeader(204)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "my-test-db"
	branch := "main"

	err = client.DatabaseBranches.DenyDemotionRequest(ctx, &DenyDemotionRequestRequest{
		Organization: org,
		Database:     name,
		Branch:       branch,
	})

	c.Assert(err, qt.IsNil)
}

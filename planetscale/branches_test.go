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
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
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
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
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
		Database:     name,
		Branch:       testBranch,
	})

	want := &DatabaseBranch{
		Name:      testBranch,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
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
		Keyspace:     "main",
	})

	want := `{}`

	c.Assert(err, qt.IsNil)
	c.Assert(vSchema.Raw, qt.DeepEquals, want)
}

func TestBranches_UpdateVSchema(t *testing.T) {
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

	vSchema, err := client.DatabaseBranches.UpdateVSchema(ctx, &UpdateBranchVschemaRequest{
		Organization: "foo",
		Database:     "bar",
		Branch:       "baz",
		Keyspace:     "bar",
		VSchema:      "{}",
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

func TestBranches_Demote(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z","production": false}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "my-test-db"
	branch := "main"

	b, err := client.DatabaseBranches.Demote(ctx, &DemoteRequest{
		Organization: org,
		Database:     name,
		Branch:       branch,
	})

	want := &DatabaseBranch{
		Name:       testBranch,
		Production: false,
		CreatedAt:  time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt:  time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(b, qt.DeepEquals, want)
}

func TestDatabaseBranches_Promote(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z","production": true}`
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
		Branch:       testBranch,
	})

	want := &DatabaseBranch{
		Name:       testBranch,
		Production: true,
		CreatedAt:  time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt:  time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabaseBranches_EnableSafeMigrations(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z","production": true,"safe_migrations":true}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.EnableSafeMigrations(ctx, &EnableSafeMigrationsRequest{
		Organization: org,
		Database:     name,
		Branch:       testBranch,
	})

	want := &DatabaseBranch{
		Name:           testBranch,
		Production:     true,
		SafeMigrations: true,
		CreatedAt:      time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt:      time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabaseBranches_DisableSafeMigrations(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z","production": true,"safe_migrations":false}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.DisableSafeMigrations(ctx, &DisableSafeMigrationsRequest{
		Organization: org,
		Database:     name,
		Branch:       testBranch,
	})

	want := &DatabaseBranch{
		Name:           testBranch,
		Production:     true,
		SafeMigrations: false,
		CreatedAt:      time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt:      time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabaseBranches_LintSchema(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `
{
			"type": "list",
			"current_page": 1,
			"next_page": null,
			"next_page_url": null,
			"prev_page": null,
			"prev_page_url": null,
			"data": [
			{
			"type": "SchemaLintError",
			"lint_error": "NO_UNIQUE_KEY",
			"subject_type": "table_error",
			"keyspace_name": "test-database",
			"table_name": "test",
			"error_description": "table \"test\" has no unique key: all tables must have at least one unique, not-null key.",
			"docs_url": "https://planetscale.com/docs/learn/change-single-unique-key",
			"column_name": "",
			"foreign_key_column_names": [],
			"auto_increment_column_names": [],
			"charset_name": "",
			"engine_name": "",
			"vindex_name": null,
			"json_path": null,
			"check_constraint_name": "",
			"enum_value": "",
			"partitioning_type": "",
			"partition_name": "",
			"url_hash": "81e9ed9a459b5824393c4fa735753c49d47861bbd43742e2baa0ab7013158d2b"
			}
			]
			}
		`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	lintErrors, err := client.DatabaseBranches.LintSchema(ctx, &LintSchemaRequest{
		Organization: org,
		Database:     name,
		Branch:       testBranch,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(len(lintErrors), qt.Equals, 1)

	want := &SchemaLintError{
		LintError:        "NO_UNIQUE_KEY",
		SubjectType:      "table_error",
		Keyspace:         "test-database",
		Table:            "test",
		ErrorDescription: "table \"test\" has no unique key: all tables must have at least one unique, not-null key.",
		DocsURL:          "https://planetscale.com/docs/learn/change-single-unique-key",
	}
	lintErr := lintErrors[0]

	c.Assert(lintErr, qt.DeepEquals, want)
}

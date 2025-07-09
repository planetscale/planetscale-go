package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

const (
	testOrg      = "my-org"
	testDatabase = "planetscale-go-test-db"
)

func TestDatabases_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db","type":"database","name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z", "region": { "slug": "us-west", "display_name": "US West" },"state":"ready"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.Create(ctx, &CreateDatabaseRequest{
		Organization: org,
		Region:       "us-west",
		Name:         name,
		Notes:        notes,
	})

	want := &Database{
		Name:  name,
		Notes: notes,
		State: DatabaseReady,
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

func TestDatabases_CreatePostgres(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db","type":"database","name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z", "region": { "slug": "us-west", "display_name": "US West" },"state":"ready","kind":"postgresql"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.Create(ctx, &CreateDatabaseRequest{
		Organization: org,
		Region:       "us-west",
		Name:         name,
		Notes:        notes,
		Kind:         "postgressql",
	})

	want := &Database{
		Name:  name,
		Notes: notes,
		State: DatabaseReady,
		Region: Region{
			Slug: "us-west",
			Name: "US West",
		},
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		Kind:      "postgresql",
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabases_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db","type":"database","name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.Get(ctx, &GetDatabaseRequest{
		Organization: org,
		Database:     name,
	})

	want := &Database{
		Name:      name,
		Notes:     notes,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabases_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"id":"planetscale-go-test-db","type":"database", "name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.List(ctx, &ListDatabasesRequest{
		Organization: org,
	})

	want := []*Database{{
		Name:      name,
		Notes:     notes,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabases_ListWithOptions(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"id":"planetscale-go-test-db","type":"database", "name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
		c.Assert(r.URL.Query().Get("page"), qt.Equals, "2")
		c.Assert(r.URL.Query().Get("per_page"), qt.Equals, "100")
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.List(ctx, &ListDatabasesRequest{
		Organization: org,
	}, WithPage(2))

	want := []*Database{{
		Name:      name,
		Notes:     notes,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabases_DeleteNoContent(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNoContent)
		_, err := w.Write(nil)
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"

	dbr, err := client.Databases.Delete(ctx, &DeleteDatabaseRequest{
		Organization: org,
		Database:     "planetscale-go-test-db",
	})

	c.Assert(err, qt.IsNil)
	c.Assert(dbr, qt.IsNil)
}

func TestDatabases_DeleteAccepted(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusAccepted)
		out := `{"id": "planetscale-go-test-db"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"

	dbr, err := client.Databases.Delete(ctx, &DeleteDatabaseRequest{
		Organization: org,
		Database:     "planetscale-go-test-db",
	})

	want := &DatabaseDeletionRequest{
		ID: "planetscale-go-test-db",
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dbr, qt.DeepEquals, want)
}

func TestDatabases_List_malformed_response(t *testing.T) {
	c := qt.New(t)

	malformedBody := `<html><head><title>400 Bad Request</title></head>
<body> <hr><center>nginx</center></body></html>`

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, err := w.Write([]byte(malformedBody))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"

	_, err = client.Databases.List(ctx, &ListDatabasesRequest{
		Organization: org,
	})

	c.Assert(err, qt.Not(qt.IsNil))
	c.Assert(err, qt.ErrorMatches, `malformed error response body received`)
}

func TestDatabases_Empty(t *testing.T) {
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

	db, err := client.Databases.List(ctx, &ListDatabasesRequest{
		Organization: org,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.HasLen, 0)
}

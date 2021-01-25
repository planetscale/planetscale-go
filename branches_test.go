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
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.DatabaseBranches.Create(ctx, org, name, &CreateDatabaseBranchRequest{
		Branch: &DatabaseBranch{
			Name:  testBranch,
			Notes: notes,
		},
	})

	want := &DatabaseBranch{
		Name:      testBranch,
		Notes:     notes,
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
		out := `{"data":[{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.DatabaseBranches.List(ctx, org, name)

	want := []*DatabaseBranch{{
		Name:      testBranch,
		Notes:     notes,
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

	db, err := client.DatabaseBranches.List(ctx, org, name)

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, []*DatabaseBranch{})
}

func TestDatabaseBranches_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"planetscale-go-test-db-branch","type":"database_branch","name":"planetscale-go-test-db-branch","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.DatabaseBranches.Get(ctx, org, name, testBranch)

	want := &DatabaseBranch{
		Name:      testBranch,
		Notes:     notes,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabaseBranches_Status(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
    "id": "development",
	"type": "database_branch_status",
	"deploy_phase": "deployed",
	"created_at": "2021-01-14T10:19:23.000Z",
	"updated_at": "2021-01-14T10:19:23.000Z",
	"mysql_gateway_host": "test-host",
	"mysql_gateway_port": 3306,
	"mysql_gateway_user": "root",
	"mysql_gateway_pass": "password"
}`

		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)

	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"

	db, err := client.DatabaseBranches.Status(ctx, org, name, testBranch)

	want := &DatabaseBranchStatus{
		DeployPhase: "deployed",
		GatewayHost: "test-host",
		GatewayPort: 3306,
		User:        "root",
		Password:    "password",
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

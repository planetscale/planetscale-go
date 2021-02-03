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

func TestSchemaSnapshots_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-id", "type": "schema_snapshot", "name": "planetscale-go-test-snapshot", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshot, err := client.SchemaSnapshots.Create(ctx, &CreateSchemaSnapshotRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Branch:       testBranch,
	})
	want := &SchemaSnapshot{
		ID:        "test-id",
		Name:      "planetscale-go-test-snapshot",
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot, qt.DeepEquals, want)
}

func TestSchemaSnapshots_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-id", "type": "schema_snapshot", "name": "planetscale-go-test-snapshot", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshot, err := client.SchemaSnapshots.Get(ctx, &GetSchemaSnapshotRequest{
		ID: "test-id",
	})
	want := &SchemaSnapshot{
		ID:        "test-id",
		Name:      "planetscale-go-test-snapshot",
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot, qt.DeepEquals, want)
}

func TestSchemaSnapshots_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":[{"id": "test-id", "type": "schema_snapshot", "name": "planetscale-go-test-snapshot", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshots, err := client.SchemaSnapshots.List(ctx, &ListSchemaSnapshotsRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Branch:       testBranch,
	})
	want := []*SchemaSnapshot{{
		ID:        "test-id",
		Name:      "planetscale-go-test-snapshot",
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshots, qt.DeepEquals, want)

}

func TestSchemaSnapshots_RequestDeploy(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.SchemaSnapshots.RequestDeploy(ctx, &SchemaSnapshotRequestDeployRequest{
		SchemaSnapshotID: "test-snapshot-id",
		IntoBranch:       "some-branch",
		Notes:            "",
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:         "test-deploy-request-id",
		Branch:     "development",
		IntoBranch: "some-branch",
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

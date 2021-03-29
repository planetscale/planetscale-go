package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestDeployRequests_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": null}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.DeployRequests.Get(ctx, &GetDeployRequestRequest{
		Organization: "test-organization",
		Database:     "test-database",
		Number:       1337,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:         "test-deploy-request-id",
		Branch:     "development",
		IntoBranch: "some-branch",
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   nil,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestDeployRequests_Deploy(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z", "deployment_state": "queued", "number": 1337}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.DeployRequests.Deploy(ctx, &PerformDeployRequest{

		Organization: "test-organization",
		Database:     "test-database",
		Number:       1337,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:              "test-deploy-request-id",
		Branch:          "development",
		IntoBranch:      "some-branch",
		Number:          1337,
		DeploymentState: "queued",
		Notes:           "",
		CreatedAt:       testTime,
		UpdatedAt:       testTime,
		ClosedAt:        &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestDeployRequests_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data": [{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	requests, err := client.DeployRequests.List(ctx, &ListDeployRequestsRequest{
		Organization: testOrg,
		Database:     testDatabase,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := []*DeployRequest{{
		ID:         "test-deploy-request-id",
		Branch:     "development",
		IntoBranch: "some-branch",
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, want)
}

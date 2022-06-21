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
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": null, "number": 1337}`
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
		Number:     1337,
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
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z", "deployment": { "state": "queued"}, "number": 1337}`
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
		ID:         "test-deploy-request-id",
		Branch:     "development",
		IntoBranch: "some-branch",
		Number:     1337,
		Deployment: &Deployment{
			State: "queued",
		},
		Notes:     "",
		CreatedAt: testTime,
		UpdatedAt: testTime,
		ClosedAt:  &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestDeployRequests_CancelDeploy(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z", "deployment": { "state": "pending" }, "number": 1337}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.DeployRequests.CancelDeploy(ctx, &CancelDeployRequestRequest{
		Organization: "test-organization",
		Database:     "test-database",
		Number:       1337,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:     "test-deploy-request-id",
		Branch: "development",
		Deployment: &Deployment{
			State: "pending",
		},
		IntoBranch: "some-branch",
		Number:     1337,
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestDeployRequests_Close(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z", "deployment": { "state": "pending" }, "number": 1337}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.DeployRequests.CloseDeploy(ctx, &CloseDeployRequestRequest{
		Organization: "test-organization",
		Database:     "test-database",
		Number:       1337,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:     "test-deploy-request-id",
		Branch: "development",
		Deployment: &Deployment{
			State: "pending",
		},
		IntoBranch: "some-branch",
		Number:     1337,
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestDeployRequests_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "number": 1337, "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	requests, err := client.DeployRequests.Create(ctx, &CreateDeployRequestRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Notes:        "",
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:         "test-deploy-request-id",
		Number:     1337,
		Branch:     "development",
		IntoBranch: "some-branch",
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, want)
}

func TestDeployRequests_Review(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-review-id","type": "DeployRequestReview","body": "test body","html_body": "","state": "approved","created_at": "2021-01-14T10:19:23.000Z","updated_at": "2021-01-14T10:19:23.000Z"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	requests, err := client.DeployRequests.CreateReview(ctx, &ReviewDeployRequestRequest{
		Organization: testOrg,
		Database:     testDatabase,
		CommentText:  "test body",
		ReviewAction: ReviewApprove,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequestReview{
		ID:        "test-review-id",
		Body:      "test body",
		State:     "approved",
		CreatedAt: testTime,
		UpdatedAt: testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(requests, qt.DeepEquals, want)
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

func TestDeployRequests_SkipRevertDeploy(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z", "deployment": { "state": "complete" }, "number": 1337}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.DeployRequests.SkipRevertDeploy(ctx, &SkipRevertDeployRequest{
		Organization: "test-organization",
		Database:     "test-database",
		Number:       1337,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:     "test-deploy-request-id",
		Branch: "development",
		Deployment: &Deployment{
			State: "complete",
		},
		IntoBranch: "some-branch",
		Number:     1337,
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

func TestDeployRequests_RevertDeploy(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id": "test-deploy-request-id", "branch": "development", "into_branch": "some-branch", "notes": "", "created_at": "2021-01-14T10:19:23.000Z", "updated_at": "2021-01-14T10:19:23.000Z", "closed_at": "2021-01-14T10:19:23.000Z", "deployment": { "state": "complete_revert" }, "number": 1337}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	dr, err := client.DeployRequests.RevertDeploy(ctx, &RevertDeployRequest{
		Organization: "test-organization",
		Database:     "test-database",
		Number:       1337,
	})

	testTime := time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC)

	want := &DeployRequest{
		ID:     "test-deploy-request-id",
		Branch: "development",
		Deployment: &Deployment{
			State: "complete_revert",
		},
		IntoBranch: "some-branch",
		Number:     1337,
		Notes:      "",
		CreatedAt:  testTime,
		UpdatedAt:  testTime,
		ClosedAt:   &testTime,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(dr, qt.DeepEquals, want)
}

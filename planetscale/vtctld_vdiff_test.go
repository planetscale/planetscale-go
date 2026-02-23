package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestVDiff_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vdiff/workflows/my-workflow/vdiffs")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")
		_, hasFilteredReplicationWaitTime := body["filtered_replication_wait_time"]
		_, hasMaxReportSampleRows := body["max_report_sample_rows"]
		_, hasMaxExtraRowsToCompare := body["max_extra_rows_to_compare"]
		_, hasRowDiffColumnTruncateAt := body["row_diff_column_truncate_at"]
		_, hasLimit := body["limit"]
		c.Assert(hasFilteredReplicationWaitTime, qt.IsFalse)
		c.Assert(hasMaxReportSampleRows, qt.IsFalse)
		c.Assert(hasMaxExtraRowsToCompare, qt.IsFalse)
		c.Assert(hasRowDiffColumnTruncateAt, qt.IsFalse)
		c.Assert(hasLimit, qt.IsFalse)

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.VDiff.Create(ctx, &VDiffCreateRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVDiff_CreateWithExplicitZeroValues(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vdiff/workflows/my-workflow/vdiffs")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")
		c.Assert(body["filtered_replication_wait_time"], qt.Equals, float64(0))
		c.Assert(body["max_report_sample_rows"], qt.Equals, float64(0))
		c.Assert(body["max_extra_rows_to_compare"], qt.Equals, float64(0))
		c.Assert(body["row_diff_column_truncate_at"], qt.Equals, float64(0))
		c.Assert(body["limit"], qt.Equals, float64(0))

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	zero := 0
	ctx := context.Background()
	data, err := client.VDiff.Create(ctx, &VDiffCreateRequest{
		Organization:                "my-org",
		Database:                    "my-db",
		Branch:                      "my-branch",
		Workflow:                    "my-workflow",
		TargetKeyspace:              "target",
		FilteredReplicationWaitTime: &zero,
		MaxReportSampleRows:         &zero,
		MaxExtraRowsToCompare:       &zero,
		RowDiffColumnTruncateAt:     &zero,
		Limit:                       &zero,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVDiff_Show(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vdiff/workflows/my-workflow/vdiffs/my-uuid")
		c.Assert(r.URL.Query().Get("target_keyspace"), qt.Equals, "target")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.VDiff.Show(ctx, &VDiffShowRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		UUID:           "my-uuid",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVDiff_Stop(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vdiff/workflows/my-workflow/vdiffs/my-uuid/stop")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.VDiff.Stop(ctx, &VDiffStopRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		UUID:           "my-uuid",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVDiff_Resume(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vdiff/workflows/my-workflow/vdiffs/my-uuid/resume")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.VDiff.Resume(ctx, &VDiffResumeRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		UUID:           "my-uuid",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestVDiff_Delete(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodDelete)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/vdiff/workflows/my-workflow/vdiffs/my-uuid")
		c.Assert(r.URL.Query().Get("target_keyspace"), qt.Equals, "target")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.VDiff.Delete(ctx, &VDiffDeleteRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		UUID:           "my-uuid",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

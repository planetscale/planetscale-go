package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestD1ImportNotifications_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/internal/organizations/my-org/databases/my-db/d1-import-notifications")

		var body createD1ImportNotificationRequest
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body.MigrationID, qt.Equals, "abc123")
		c.Assert(body.Event, qt.Equals, "start")
		c.Assert(body.BranchName, qt.Equals, "main")
		c.Assert(body.Method, qt.Equals, "pgloader")
		c.Assert(body.ExportBytes, qt.Equals, int64(1024))
		c.Assert(body.TableCount, qt.Equals, 3)

		w.WriteHeader(http.StatusAccepted)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	err = client.D1ImportNotifications.Create(context.Background(), &CreateD1ImportNotificationRequest{
		Organization: "my-org",
		Database:     "my-db",
		BranchName:   "main",
		MigrationID:  "abc123",
		Event:        "start",
		Method:       "pgloader",
		ExportBytes:  1024,
		TableCount:   3,
	})
	c.Assert(err, qt.IsNil)
}

func TestD1ImportNotifications_CreateOmitsEmptyBranchName(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body createD1ImportNotificationRequest
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body.BranchName, qt.Equals, "")

		w.WriteHeader(http.StatusAccepted)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	err = client.D1ImportNotifications.Create(context.Background(), &CreateD1ImportNotificationRequest{
		Organization: "my-org",
		Database:     "my-db",
		MigrationID:  "abc123",
		Event:        "complete",
	})
	c.Assert(err, qt.IsNil)
}

package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestResetDefaultRole(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
			"id": "role-id",
			"name": "default-postgres-role",
			"access_host_url": "pg.psdb.cloud",
			"database_name": "postgres",
			"password": "secure-password",
			"actor": {"id": "actor-id", "display_name": "actor-name"},
			"username": "postgres",
			"created_at": "2025-07-15T10:19:23.000Z"
		}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	role, err := client.PostgresRoles.ResetDefaultRole(ctx, ResetDefaultRoleRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	})

	want := &PostgresRole{
		ID:            "role-id",
		Name:          "default-postgres-role",
		AccessHostURL: "pg.psdb.cloud",
		DatabaseName:  "postgres",
		Password:      "secure-password",
		Actor:         Actor{ID: "actor-id", Name: "actor-name"},
		Username:      "postgres",
		CreatedAt:     time.Date(2025, time.July, 15, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(role, qt.DeepEquals, want)
}

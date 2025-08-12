package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

const testRoleID = "AbC123xYz"

func TestResetDefaultRole(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "POST")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles/reset-default")
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

	role, err := client.PostgresRoles.ResetDefaultRole(ctx, &ResetDefaultRoleRequest{
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

func TestPostgresRoles_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "GET")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles")
		w.WriteHeader(200)
		out := `{
    "data":
    [
        {
            "id": "AbC123xYz",
            "name": "test-role",
            "access_host_url": "test.planetscale.com",
            "database_name": "test-db",
            "username": "test-user",
            "created_at": "2021-01-14T10:19:23.000Z"
        }
    ]
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

	roles, err := client.PostgresRoles.List(ctx, &ListPostgresRolesRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	})

	want := []*PostgresRole{
		{
			ID:            testRoleID,
			Name:          "test-role",
			AccessHostURL: "test.planetscale.com",
			DatabaseName:  "test-db",
			Username:      "test-user",
			CreatedAt:     time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(roles, qt.DeepEquals, want)
}

func TestPostgresRoles_ListEmpty(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "GET")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles")
		w.WriteHeader(200)
		out := `{"data":[]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	roles, err := client.PostgresRoles.List(ctx, &ListPostgresRolesRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(roles, qt.HasLen, 0)
}

func TestPostgresRoles_ListWithPagination(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "GET")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles")
		// Verify pagination parameters are included in the request
		c.Assert(r.URL.Query().Get("page"), qt.Equals, "2")
		c.Assert(r.URL.Query().Get("per_page"), qt.Equals, "50")

		w.WriteHeader(200)
		out := `{
    "data":
    [
        {
            "id": "AbC123xYz",
            "name": "test-role",
            "access_host_url": "test.planetscale.com",
            "database_name": "test-db",
            "username": "test-user",
            "created_at": "2021-01-14T10:19:23.000Z"
        }
    ]
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

	roles, err := client.PostgresRoles.List(ctx, &ListPostgresRolesRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	}, WithPage(2), WithPerPage(50))

	want := []*PostgresRole{
		{
			ID:            testRoleID,
			Name:          "test-role",
			AccessHostURL: "test.planetscale.com",
			DatabaseName:  "test-db",
			Username:      "test-user",
			CreatedAt:     time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(roles, qt.DeepEquals, want)
}

func TestPostgresRoles_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "GET")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles/AbC123xYz")
		w.WriteHeader(200)
		out := fmt.Sprintf(`{
    "id": "%s",
    "name": "test-role",
    "access_host_url": "test.planetscale.com",
    "database_name": "test-db",
    "password": "secret-password",
    "username": "test-user",
    "created_at": "2021-01-14T10:19:23.000Z"
}`, testRoleID)
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	role, err := client.PostgresRoles.Get(ctx, &GetPostgresRoleRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		RoleId:       testRoleID,
	})

	want := &PostgresRole{
		ID:            testRoleID,
		Name:          "test-role",
		AccessHostURL: "test.planetscale.com",
		DatabaseName:  "test-db",
		Password:      "secret-password",
		Username:      "test-user",
		CreatedAt:     time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(role, qt.DeepEquals, want)
}

func TestPostgresRoles_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "POST")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles")
		
		// Verify request body
		body, err := io.ReadAll(r.Body)
		c.Assert(err, qt.IsNil)
		var reqBody CreatePostgresRoleRequest
		err = json.Unmarshal(body, &reqBody)
		c.Assert(err, qt.IsNil)
		c.Assert(reqBody.Name, qt.Equals, "new-role")
		c.Assert(reqBody.TTL, qt.Equals, 3600)
		c.Assert(reqBody.InheritedRoles, qt.DeepEquals, []string{"pg_read_all_data", "pg_write_all_data"})
		
		w.WriteHeader(200)
		out := fmt.Sprintf(`{
    "id": "%s",
    "name": "new-role",
    "access_host_url": "test.planetscale.com",
    "database_name": "test-db",
    "password": "generated-password",
    "username": "new-user",
    "created_at": "2021-01-14T10:19:23.000Z"
}`, testRoleID)
		_, writeErr := w.Write([]byte(out))
		c.Assert(writeErr, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	role, err := client.PostgresRoles.Create(ctx, &CreatePostgresRoleRequest{
		Organization:   org,
		Database:       db,
		Branch:         branch,
		Name:           "new-role",
		TTL:            3600,
		InheritedRoles: []string{"pg_read_all_data", "pg_write_all_data"},
	})

	want := &PostgresRole{
		ID:            testRoleID,
		Name:          "new-role",
		AccessHostURL: "test.planetscale.com",
		DatabaseName:  "test-db",
		Password:      "generated-password",
		Username:      "new-user",
		CreatedAt:     time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(role, qt.DeepEquals, want)
}

func TestPostgresRoles_Update(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "PATCH")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles/AbC123xYz")
		
		// Verify request body
		body, err := io.ReadAll(r.Body)
		c.Assert(err, qt.IsNil)
		var reqBody UpdatePostgresRoleRequest
		err = json.Unmarshal(body, &reqBody)
		c.Assert(err, qt.IsNil)
		c.Assert(reqBody.Name, qt.Equals, "updated-role")
		
		w.WriteHeader(200)
		out := fmt.Sprintf(`{
    "id": "%s",
    "name": "updated-role",
    "access_host_url": "test.planetscale.com",
    "database_name": "test-db",
    "password": "existing-password",
    "username": "existing-user",
    "created_at": "2021-01-14T10:19:23.000Z"
}`, testRoleID)
		_, writeErr := w.Write([]byte(out))
		c.Assert(writeErr, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	role, err := client.PostgresRoles.Update(ctx, &UpdatePostgresRoleRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		RoleId:       testRoleID,
		Name:         "updated-role",
	})

	want := &PostgresRole{
		ID:            testRoleID,
		Name:          "updated-role",
		AccessHostURL: "test.planetscale.com",
		DatabaseName:  "test-db",
		Password:      "existing-password",
		Username:      "existing-user",
		CreatedAt:     time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(role, qt.DeepEquals, want)
}

func TestPostgresRoles_Renew(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "POST")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles/AbC123xYz/renew")
		w.WriteHeader(200)
		out := fmt.Sprintf(`{
    "id": "%s",
    "name": "existing-role",
    "access_host_url": "test.planetscale.com",
    "database_name": "test-db",
    "password": "renewed-password",
    "username": "existing-user",
    "created_at": "2021-01-14T10:19:23.000Z"
}`, testRoleID)
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	role, err := client.PostgresRoles.Renew(ctx, &RenewPostgresRoleRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		RoleId:       testRoleID,
	})

	want := &PostgresRole{
		ID:            testRoleID,
		Name:          "existing-role",
		AccessHostURL: "test.planetscale.com",
		DatabaseName:  "test-db",
		Password:      "renewed-password",
		Username:      "existing-user",
		CreatedAt:     time.Date(2021, time.January, 14, 10, 19, 23, 0, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(role, qt.DeepEquals, want)
}

func TestPostgresRoles_Delete(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, "DELETE")
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/roles/AbC123xYz")
		
		// Verify request body
		body, err := io.ReadAll(r.Body)
		c.Assert(err, qt.IsNil)
		var reqBody DeletePostgresRoleRequest
		err = json.Unmarshal(body, &reqBody)
		c.Assert(err, qt.IsNil)
		c.Assert(reqBody.Successor, qt.Equals, "default")
		
		w.WriteHeader(204)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	err = client.PostgresRoles.Delete(ctx, &DeletePostgresRoleRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		RoleId:       testRoleID,
		Successor:    "default",
	})

	c.Assert(err, qt.IsNil)
}

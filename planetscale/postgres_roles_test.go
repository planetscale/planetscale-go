package planetscale

import (
	"context"
	"fmt"
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
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
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
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
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
	})

	c.Assert(err, qt.IsNil)
}

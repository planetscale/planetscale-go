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

const testPasswordID = "4rwwvrxk2o99" // #nosec G101 - Not a password but a password identifier.

func TestPasswords_Create(t *testing.T) {
	c := qt.New(t)
	plainText := "plain-text-password"
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := fmt.Sprintf(`{
    "id": "%s",
    "role": "admin",
    "plain_text": "%s",
    "name": "planetscale-go-test-password",
    "created_at": "2021-01-14T10:19:23.000Z"
}`, testPasswordID, plainText)
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	branch := "my-branch"

	password, err := client.Passwords.Create(ctx, &DatabaseBranchPasswordRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		Role:         "admin",
	})

	want := &DatabaseBranchPassword{
		Name:     "planetscale-go-test-password",
		PublicID: testPasswordID,

		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		Role:      "admin",
		PlainText: plainText,
	}

	c.Assert(err, qt.IsNil)
	c.Assert(password, qt.DeepEquals, want)
}

func TestPasswords_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
    "data":
    [
        {
            "id": "4rwwvrxk2o99",
            "name": "planetscale-go-test-password",
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
	db := "planetscale-go-test-db"

	passwords, err := client.Passwords.List(ctx, &ListDatabaseBranchPasswordRequest{
		Organization: org,
		Database:     db,
	})

	want := []*DatabaseBranchPassword{
		{
			Name:      "planetscale-go-test-password",
			PublicID:  testPasswordID,
			CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(passwords, qt.DeepEquals, want)
}

func TestPasswords_ListBranch(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
    "data":
    [
        {
            "id": "4rwwvrxk2o99",
            "name": "planetscale-go-test-password",
            "database_branch": {
			  "name": "my-branch"
			},
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
	db := "planetscale-go-test-db"
	branch := "my-branch"

	passwords, err := client.Passwords.List(ctx, &ListDatabaseBranchPasswordRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
	})

	want := []*DatabaseBranchPassword{
		{
			Name: "planetscale-go-test-password",
			Branch: DatabaseBranch{
				Name: branch,
			},
			PublicID:  testPasswordID,
			CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(passwords, qt.DeepEquals, want)
}

func TestPasswords_ListEmpty(t *testing.T) {
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
	db := "planetscale-go-test-db"

	passwords, err := client.Passwords.List(ctx, &ListDatabaseBranchPasswordRequest{
		Organization: org,
		Database:     db,
	})

	c.Assert(err, qt.IsNil)
	c.Assert(passwords, qt.HasLen, 0)
}

func TestPasswords_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := fmt.Sprintf(`{
    "id": "%s",
    "role": "writer",
    "name": "planetscale-go-test-password",
    "created_at": "2021-01-14T10:19:23.000Z"
}`, testPasswordID)
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	db := "planetscale-go-test-db"
	branch := "my-branch"

	password, err := client.Passwords.Get(ctx, &GetDatabaseBranchPasswordRequest{
		Organization: org,
		Database:     db,
		Branch:       branch,
		PasswordId:   testPasswordID,
	})

	want := &DatabaseBranchPassword{
		Name:      "planetscale-go-test-password",
		PublicID:  testPasswordID,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		Role:      "writer",
	}

	c.Assert(err, qt.IsNil)
	c.Assert(password, qt.DeepEquals, want)
}

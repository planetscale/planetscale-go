package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestDatabases_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":{"id":"planetscale-go-test-db","type":"database","attributes":{"name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.Create(ctx, org, &CreateDatabaseRequest{
		Database: &Database{
			Name:  name,
			Notes: notes,
		},
	})

	want := &Database{
		Name:      name,
		Notes:     notes,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

func TestDatabases_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"data":{"id":"planetscale-go-test-db","type":"database","attributes":{"name":"planetscale-go-test-db","notes":"This is a test DB created from the planetscale-go API library","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"}}}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	org := "my-org"
	name := "planetscale-go-test-db"
	notes := "This is a test DB created from the planetscale-go API library"

	db, err := client.Databases.Get(ctx, org, name)

	want := &Database{
		Name:      name,
		Notes:     notes,
		CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
		UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(db, qt.DeepEquals, want)
}

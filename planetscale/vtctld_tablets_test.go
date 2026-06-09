package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestVtctld_ListTablets(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/tablets")

		// No filters set, so none should be present on the request.
		c.Assert(r.URL.Query().Get("keyspace"), qt.Equals, "")
		c.Assert(r.URL.Query().Get("shard"), qt.Equals, "")
		c.Assert(r.URL.Query().Get("tablet_type"), qt.Equals, "")
		c.Assert(r.URL.Query().Get("tablet_alias"), qt.Equals, "")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`[{"type":"primary","keyspace":"commerce","shard":"-","tablets":[{"alias":"zone1-100","role":"primary","cell":"zone1"}]}]`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	groups, err := client.Vtctld.ListTablets(ctx, &ListBranchTabletsRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(groups, qt.DeepEquals, []*TabletGroup{
		{
			Type:     "primary",
			Keyspace: "commerce",
			Shard:    "-",
			Tablets: []Tablet{
				{Alias: "zone1-100", Role: "primary", Cell: "zone1"},
			},
		},
	})
}

func TestVtctld_ListTablets_Filters(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/tablets")
		c.Assert(r.URL.Query().Get("keyspace"), qt.Equals, "commerce")
		c.Assert(r.URL.Query().Get("shard"), qt.Equals, "-80")
		c.Assert(r.URL.Query().Get("tablet_type"), qt.Equals, "replica")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`[]`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	_, err = client.Vtctld.ListTablets(ctx, &ListBranchTabletsRequest{
		Organization: "my-org",
		Database:     "my-db",
		Branch:       "my-branch",
		Keyspace:     "commerce",
		Shard:        "-80",
		TabletType:   "replica",
	})
	c.Assert(err, qt.IsNil)
}

func TestVtctld_ListTablets_TabletAliases(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/tablets")
		// Multiple aliases are sent as a single comma-separated value.
		c.Assert(r.URL.Query().Get("tablet_alias"), qt.Equals, "zone1-0000000100,zone1-0000000101")

		w.WriteHeader(200)
		_, err := w.Write([]byte(`[]`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	_, err = client.Vtctld.ListTablets(ctx, &ListBranchTabletsRequest{
		Organization:  "my-org",
		Database:      "my-db",
		Branch:        "my-branch",
		TabletAliases: []string{"zone1-0000000100", "zone1-0000000101"},
	})
	c.Assert(err, qt.IsNil)
}

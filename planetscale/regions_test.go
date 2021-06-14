package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestRegions_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
"data": [
		{
			"id": "my-cool-org",
			"type": "Region",
			"slug": "us-east",
			"display_name": "US East",
			"enabled": true
		}
	]
}`

		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	orgs, err := client.Regions.List(ctx, &ListRegionsRequest{})

	c.Assert(err, qt.IsNil)
	want := []*Region{
		{
			Name:    "US East",
			Slug:    "us-east",
			Enabled: true,
		},
	}

	c.Assert(orgs, qt.DeepEquals, want)
}

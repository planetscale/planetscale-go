package planetscale

import (
	"context"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestServiceTokens_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"id":"test-id","type":"ServiceToken","token":"d2980bbd91a4ab878601ef0573a7af7b1b15e705"}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshot, err := client.ServiceTokens.Create(ctx, &CreateServiceTokenRequest{
		Organization: testOrg,
	})
	want := &ServiceToken{
		ID:    "test-id",
		Type:  "ServiceToken",
		Token: "d2980bbd91a4ab878601ef0573a7af7b1b15e705",
	}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot, qt.DeepEquals, want)
}

func TestServiceTokens_List(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"type":"list","next_page":null,"prev_page":null,"data":[{"id":"txhc257pxjuc","type":"ServiceToken","token":null}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshot, err := client.ServiceTokens.List(ctx, &ListServiceTokensRequest{
		Organization: testOrg,
	})
	want := []*ServiceToken{
		{
			ID:   "txhc257pxjuc",
			Type: "ServiceToken",
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot, qt.DeepEquals, want)
}

func TestServiceTokens_Delete(t *testing.T) {
	c := qt.New(t)

	wantURL := "/v1/organizations/my-org/service-tokens/1234"

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		c.Assert(r.URL.String(), qt.DeepEquals, wantURL)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	err = client.ServiceTokens.Delete(ctx, &DeleteServiceTokenRequest{
		Organization: testOrg,
		ID:           "1234",
	})

	c.Assert(err, qt.IsNil)
}

func TestServiceTokens_GetAccess(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{"type":"list","next_page":null,"prev_page":null,"data":[{"id":"hjqui654yu71","type":"DatabaseAccess","resource":{"id":"1lbjwnp48b6r","type":"Database","url":"http://api.planetscaledb.local:3000/v1/organizations/organization5/databases/hidden-river-4209","branches_url":"http://api.planetscaledb.local:3000/v1/organizations/organization5/databases/hidden-river-4209/branches","name":"hidden-river-4209","notes":"","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"},"access":"read_comment"}]}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshot, err := client.ServiceTokens.GetAccess(ctx, &GetServiceTokenAccessRequest{
		Organization: testOrg,
		ID:           "1234",
	})
	want := []*ServiceTokenAccess{
		{
			ID:     "hjqui654yu71",
			Access: "read_comment",
			Type:   "DatabaseAccess",
			Resource: Database{
				Name:      "hidden-river-4209",
				Notes:     "",
				CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
				UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
			},
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot, qt.DeepEquals, want)
}

func TestServiceTokens_AddAccess(t *testing.T) {
	c := qt.New(t)

	wantBody := []byte("{\"database\":\"hidden-river-4209\",\"access\":[\"read_comment\"]}\n")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data, err := ioutil.ReadAll(r.Body)
		c.Assert(err, qt.IsNil)
		c.Assert(data, qt.DeepEquals, wantBody)

		out := `{"type":"list","next_page":null,"prev_page":null,"data":[{"id":"hjqui654yu71","type":"DatabaseAccess","resource":{"id":"1lbjwnp48b6r","type":"Database","url":"http://api.planetscaledb.local:3000/v1/organizations/organization5/databases/hidden-river-4209","branches_url":"http://api.planetscaledb.local:3000/v1/organizations/organization5/databases/hidden-river-4209/branches","name":"hidden-river-4209","notes":"","created_at":"2021-01-14T10:19:23.000Z","updated_at":"2021-01-14T10:19:23.000Z"},"access":"read_comment"}]}`
		_, err = w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	snapshot, err := client.ServiceTokens.AddAccess(ctx, &AddServiceTokenAccessRequest{
		Organization: testOrg,
		ID:           "1234",
		Database:     "hidden-river-4209",
		Accesses:     []string{"read_comment"},
	})
	want := []*ServiceTokenAccess{
		{
			ID:     "hjqui654yu71",
			Access: "read_comment",
			Type:   "DatabaseAccess",
			Resource: Database{
				Name:      "hidden-river-4209",
				Notes:     "",
				CreatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
				UpdatedAt: time.Date(2021, time.January, 14, 10, 19, 23, 000, time.UTC),
			},
		},
	}

	c.Assert(err, qt.IsNil)
	c.Assert(snapshot, qt.DeepEquals, want)
}

func TestServiceTokens_DeleteAccess(t *testing.T) {
	c := qt.New(t)

	wantBody := []byte("{\"database\":\"hidden-river-4209\",\"access\":[\"read_comment\"]}\n")

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		data, err := ioutil.ReadAll(r.Body)
		c.Assert(err, qt.IsNil)
		c.Assert(data, qt.DeepEquals, wantBody)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	err = client.ServiceTokens.DeleteAccess(ctx, &DeleteServiceTokenAccessRequest{
		Organization: testOrg,
		ID:           "1234",
		Database:     "hidden-river-4209",
		Accesses:     []string{"read_comment"},
	})

	c.Assert(err, qt.IsNil)
}

package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
	"github.com/google/jsonapi"
)

func TestDo(t *testing.T) {
	tests := []struct {
		desc          string
		response      string
		statusCode    int
		method        string
		expectedError error
		body          interface{}
		v             interface{}
		want          interface{}
	}{
		{
			desc:       "returns an HTTP response and no error for 2xx responses",
			statusCode: http.StatusOK,
			response:   `{}`,
			method:     http.MethodGet,
		},
		{
			desc:       "returns ErrorResponse for 4xx errors",
			statusCode: http.StatusNotFound,
			method:     http.MethodGet,
			response: `{
				"code": "not_found",
				"message": "Not Found"
			}`,
			expectedError: &ErrorResponse{
				Code:    "not_found",
				Message: "Not Found",
			},
		},
		{
			desc:       "returns an HTTP response 200 when posting a request",
			statusCode: http.StatusOK,
			response: `
{ 
"data": {
      "id": "509",
      "type": "database",
      "attributes": {
        "name": "foo-bar",
        "notes": ""
      }
    }
}`,
			body: &Database{
				Name: "foo-bar",
			},
			v: &Database{},
			want: &Database{
				Name: "foo-bar",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			c := qt.New(t)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)

				_, err := w.Write([]byte(tt.response))
				if err != nil {
					t.Fatal(err)
				}
			}))
			t.Cleanup(ts.Close)

			client, err := NewClient(WithBaseURL(ts.URL))
			if err != nil {
				t.Fatal(err)
			}

			req, err := client.newRequest(tt.method, "/api-endpoint", tt.body)
			if err != nil {
				t.Fatal(err)
			}

			res, err := client.Do(context.Background(), req)
			if err != nil && tt.expectedError == nil {
				if tt.expectedError != nil {
					c.Assert(tt.expectedError, qt.DeepEquals, err)
				} else {
					c.Assert(err, qt.IsNil)
				}

			}
			defer res.Body.Close()

			if tt.v != nil {
				err = jsonapi.UnmarshalPayload(res.Body, tt.v)
				if err != nil {
					t.Fatal(err)
				}
			}

			c.Assert(tt.expectedError, qt.DeepEquals, err)
			c.Assert(res, qt.Not(qt.IsNil))
			c.Assert(res.StatusCode, qt.Equals, tt.statusCode)
			c.Assert(tt.want, qt.DeepEquals, tt.v)
		})
	}
}

package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
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
			expectedError: &Error{
				msg:  "Not Found",
				Code: ErrNotFound,
			},
		},
		{
			desc:       "returns an HTTP response 200 when posting a request",
			statusCode: http.StatusOK,
			response: `
{ 
	"id": "509",
	"type": "database",
	"name": "foo-bar",
	"notes": ""
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
			ctx := context.Background()
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

			res, err := client.client.Do(req)
			c.Assert(err, qt.IsNil)
			defer res.Body.Close()

			err = client.handleResponse(ctx, res, &tt.v)
			if err != nil {
				if tt.expectedError != nil {
					c.Assert(tt.expectedError.Error(), qt.Equals, err.Error())
				}
			}

			c.Assert(res, qt.Not(qt.IsNil))
			c.Assert(res.StatusCode, qt.Equals, tt.statusCode)

			if tt.v != nil && tt.want != nil {
				c.Assert(tt.want, qt.DeepEquals, tt.v)
			}
		})
	}
}

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
		clientOptions []ClientOption
		wantHeaders   map[string]string
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
			desc:          "sets a custom header with the request option",
			statusCode:    http.StatusOK,
			response:      `{}`,
			method:        http.MethodGet,
			clientOptions: []ClientOption{WithUserAgent("test-user-agent"), WithRequestHeaders(map[string]string{"Test-Header": "test-value"})},
			wantHeaders: map[string]string{
				"Test-Header": "test-value",
				"User-Agent":  "test-user-agent planetscale-go/v0.108.0",
			},
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
			desc:       "returns ErrorResponse for 5xx errors",
			statusCode: http.StatusInternalServerError,
			method:     http.MethodGet,
			response:   `{}`,
			expectedError: &Error{
				msg:  "internal error, response body doesn't match error type signature",
				Code: ErrInternal,
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
		{
			desc:       "returns an HTTP response 204 when deleting a request",
			statusCode: http.StatusNoContent,
			method:     http.MethodDelete,
			response:   "",
			body:       nil,
			v:          &Database{},
			want:       nil,
		},
		{
			desc:       "returns an non-204 HTTP response when deleting a request",
			statusCode: http.StatusAccepted,
			method:     http.MethodDelete,
			response: `{
			"id": "test"
			}`,
			body: nil,
			v:    &DatabaseDeletionRequest{},
			want: &DatabaseDeletionRequest{
				ID: "test",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.desc, func(t *testing.T) {
			ctx := context.Background()
			c := qt.New(t)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(tt.statusCode)

				if tt.wantHeaders != nil {
					for key, value := range tt.wantHeaders {
						c.Assert(r.Header.Get(key), qt.Equals, value)
					}
				}

				res := []byte(tt.response)
				if tt.response == "" {
					res = nil
				}
				_, err := w.Write(res)
				if err != nil {
					t.Fatal(err)
				}
			}))
			t.Cleanup(ts.Close)

			opts := append(tt.clientOptions, WithBaseURL(ts.URL))
			client, err := NewClient(opts...)
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

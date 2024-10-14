package planetscale

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"runtime/debug"
	"strconv"

	"github.com/hashicorp/go-cleanhttp"
	"github.com/pkg/errors"
	"golang.org/x/oauth2"
)

const (
	DefaultBaseURL = "https://api.planetscale.com/"
	jsonMediaType  = "application/json"
)

// ErrorCode defines the code of an error.
type ErrorCode string

const (
	ErrInternal          ErrorCode = "internal"           // Internal error.
	ErrInvalid           ErrorCode = "invalid"            // Invalid operation, e.g wrong params
	ErrPermission        ErrorCode = "permission"         // Permission denied.
	ErrNotFound          ErrorCode = "not_found"          // Resource not found.
	ErrRetry             ErrorCode = "retry"              // Operation should be retried.
	ErrResponseMalformed ErrorCode = "response_malformed" // Response body is malformed.
)

// Client encapsulates a client that talks to the PlanetScale API
type Client struct {
	// client represents the HTTP client used for making HTTP requests.
	client *http.Client

	// UserAgent is the version of the planetscale-go library that is being used
	UserAgent string

	// headers are used to override request headers for every single HTTP request
	headers map[string]string

	// base URL for the API
	baseURL *url.URL

	AuditLogs        AuditLogsService
	Backups          BackupsService
	Databases        DatabasesService
	DatabaseBranches DatabaseBranchesService
	DataImports      DataImportsService
	Organizations    OrganizationsService
	Passwords        PasswordsService
	Regions          RegionsService
	DeployRequests   DeployRequestsService
	ServiceTokens    ServiceTokenService
	Keyspaces        KeyspacesService
}

// ListOptions are options for listing responses.
type ListOptions struct {
	URLValues *url.Values
}

type ListOption func(*ListOptions) error

// DefaultListOptions returns the default list options values.
func defaultListOptions(opts ...ListOption) *ListOptions {
	listOpts := &ListOptions{
		URLValues: &url.Values{},
	}

	for _, opt := range opts {
		err := opt(listOpts)
		if err != nil {
			panic(err)
		}
	}

	return listOpts
}

// WithStartingAfter returns a ListOption that sets the "starting_after" URL parameter.
func WithStartingAfter(startingAfter string) ListOption {
	return func(opt *ListOptions) error {
		if startingAfter != "" {
			opt.URLValues.Set("starting_after", startingAfter)
		}
		return nil
	}
}

// WithLimit returns a ListOption that sets the "limit" URL parameter.
func WithLimit(limit int) ListOption {
	return func(opt *ListOptions) error {
		if limit > 0 {
			limitStr := strconv.Itoa(limit)
			opt.URLValues.Set("limit", limitStr)
		}
		return nil
	}
}

// WithPage returns a ListOption that sets the "page" URL parameter.
func WithPage(page int) ListOption {
	return func(opt *ListOptions) error {
		if page > 0 {
			pageStr := strconv.Itoa(page)
			opt.URLValues.Set("page", pageStr)
		}
		return nil
	}
}

// WithPerPage returns a ListOption that sets the "per_page" URL paramter.
func WithPerPage(perPage int) ListOption {
	return func(opt *ListOptions) error {
		if perPage > 0 {
			perPageStr := strconv.Itoa(perPage)
			opt.URLValues.Set("per_page", perPageStr)
		}
		return nil
	}
}

// ClientOption provides a variadic option for configuring the client
type ClientOption func(c *Client) error

func defaultUserAgent() string {
	libraryVersion := "unknown"
	buildInfo, ok := debug.ReadBuildInfo()
	if ok {
		for _, dep := range buildInfo.Deps {
			if dep.Path == "github.com/planetscale/planetscale-go" {
				libraryVersion = dep.Version
				break
			}
		}
	}

	return "planetscale-go/" + libraryVersion
}

// WithUserAgent overrides the User-Agent header.
func WithUserAgent(userAgent string) ClientOption {
	return func(c *Client) error {
		c.UserAgent = fmt.Sprintf("%s %s", userAgent, c.UserAgent)
		return nil
	}
}

// WithRequestHeaders sets the request headers for every HTTP request.
func WithRequestHeaders(headers map[string]string) ClientOption {
	return func(c *Client) error {
		for k, v := range headers {
			c.headers[k] = v
		}

		return nil
	}
}

// WithBaseURL overrides the base URL for the API.
func WithBaseURL(baseURL string) ClientOption {
	return func(c *Client) error {
		parsedURL, err := url.Parse(baseURL)
		if err != nil {
			return err
		}

		c.baseURL = parsedURL
		return nil
	}
}

// WithAccessToken configures a client with the given PlanetScale access token.
func WithAccessToken(token string) ClientOption {
	return func(c *Client) error {
		if token == "" {
			return errors.New("missing access token")
		}

		tokenSource := oauth2.StaticTokenSource(&oauth2.Token{AccessToken: token})

		// make sure we use our own HTTP client
		ctx := context.WithValue(context.Background(), oauth2.HTTPClient, c.client)
		oauthClient := oauth2.NewClient(ctx, tokenSource)

		c.client = oauthClient
		return nil
	}
}

// WithServiceToken configures a client with the given PlanetScale Service Token
func WithServiceToken(name, token string) ClientOption {
	return func(c *Client) error {
		if token == "" || name == "" {
			return errors.New("missing token name and string")
		}

		transport := serviceTokenTransport{
			rt:        c.client.Transport,
			token:     token,
			tokenName: name,
		}

		c.client.Transport = &transport
		return nil
	}
}

// WithHTTPClient configures the PlanetScale client with the given HTTP client.
func WithHTTPClient(client *http.Client) ClientOption {
	return func(c *Client) error {
		if client == nil {
			client = cleanhttp.DefaultClient()
		}

		c.client = client
		return nil
	}
}

// NewClient instantiates an instance of the PlanetScale API client.
func NewClient(opts ...ClientOption) (*Client, error) {
	baseURL, err := url.Parse(DefaultBaseURL)
	if err != nil {
		return nil, err
	}

	c := &Client{
		client:    cleanhttp.DefaultClient(),
		baseURL:   baseURL,
		UserAgent: defaultUserAgent(),
		headers:   make(map[string]string, 0),
	}

	for _, opt := range opts {
		err := opt(c)
		if err != nil {
			return nil, err
		}
	}

	c.AuditLogs = &auditlogsService{client: c}
	c.Backups = &backupsService{client: c}
	c.Databases = &databasesService{client: c}
	c.DatabaseBranches = &databaseBranchesService{client: c}
	c.DataImports = &dataImportsService{client: c}
	c.Organizations = &organizationsService{client: c}
	c.Passwords = &passwordsService{client: c}
	c.Regions = &regionsService{client: c}
	c.DeployRequests = &deployRequestsService{client: c}
	c.ServiceTokens = &serviceTokenService{client: c}
	c.Keyspaces = &keyspacesService{client: c}

	return c, nil
}

// do makes an HTTP request and populates the given struct v from the response.
func (c *Client) do(ctx context.Context, req *http.Request, v interface{}) error {
	req = req.WithContext(ctx)
	res, err := c.client.Do(req)
	if err != nil {
		return err
	}
	defer res.Body.Close()

	return c.handleResponse(ctx, res, v)
}

// handleResponse makes an HTTP request and populates the given struct v from
// the response.  This is meant for internal testing and shouldn't be used
// directly. Instead please use `Client.do`.
func (c *Client) handleResponse(ctx context.Context, res *http.Response, v interface{}) error {
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return err
	}

	if res.StatusCode >= 400 {
		// errorResponse represents an error response from the API
		type errorResponse struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}

		errorRes := &errorResponse{}
		err = json.Unmarshal(out, errorRes)
		if err != nil {
			var jsonErr *json.SyntaxError
			if errors.As(err, &jsonErr) {
				return &Error{
					msg:  "malformed error response body received",
					Code: ErrResponseMalformed,
					Meta: map[string]string{
						"body":        string(out),
						"err":         jsonErr.Error(),
						"http_status": http.StatusText(res.StatusCode),
					},
				}
			}
			return err
		}

		// json.Unmarshal doesn't return an error if the response
		// body has a different protocol then "ErrorResponse". We
		// check here to make sure that errorRes is populated. If
		// not, we return the full response back to the user, so
		// they can debug the issue.
		// TODO(fatih): fix the behavior on the API side
		if *errorRes == (errorResponse{}) {
			return &Error{
				msg:  "internal error, response body doesn't match error type signature",
				Code: ErrInternal,
				Meta: map[string]string{
					"body":        string(out),
					"http_status": http.StatusText(res.StatusCode),
				},
			}
		}

		var errCode ErrorCode
		switch errorRes.Code {
		case "not_found":
			errCode = ErrNotFound
		case "unauthorized":
			errCode = ErrPermission
		case "invalid_params":
			errCode = ErrInvalid
		case "unprocessable":
			errCode = ErrRetry
		}

		return &Error{
			msg:  errorRes.Message,
			Code: errCode,
		}
	}

	// this means we don't care about unmarshaling the response body into v
	if v == nil || res.StatusCode == http.StatusNoContent {
		return nil
	}

	err = json.Unmarshal(out, &v)
	if err != nil {
		var jsonErr *json.SyntaxError
		if errors.As(err, &jsonErr) {
			return &Error{
				msg:  "malformed response body received",
				Code: ErrResponseMalformed,
				Meta: map[string]string{
					"body":        string(out),
					"http_status": http.StatusText(res.StatusCode),
				},
			}
		}
		return err
	}

	return nil
}

func (c *Client) newRequest(method string, path string, body interface{}) (*http.Request, error) {
	u, err := c.baseURL.Parse(path)
	if err != nil {
		return nil, err
	}

	var req *http.Request
	switch method {
	case http.MethodGet:
		req, err = http.NewRequest(method, u.String(), nil)
		if err != nil {
			return nil, err
		}
	default:
		buf := new(bytes.Buffer)
		if body != nil {
			err = json.NewEncoder(buf).Encode(body)
			if err != nil {
				return nil, err
			}
		}

		req, err = http.NewRequest(method, u.String(), buf)
		if err != nil {
			return nil, err
		}

		req.Header.Set("Content-Type", jsonMediaType)
	}

	req.Header.Set("Accept", jsonMediaType)
	req.Header.Set("User-Agent", c.UserAgent)

	for k, v := range c.headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

type serviceTokenTransport struct {
	rt        http.RoundTripper
	token     string
	tokenName string
}

func (t *serviceTokenTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req.Header.Add("Authorization", t.tokenName+":"+t.token)
	return t.rt.RoundTrip(req)
}

// Error represents common errors originating from the Client.
type Error struct {
	// msg contains the human readable string
	msg string

	// Code specifies the error code. i.e; NotFound, RateLimited, etc...
	Code ErrorCode

	// Meta contains additional information depending on the error code. As an
	// example, if the Code is "ErrResponseMalformed", the map will be: ["body"]
	// = "body of the response"
	Meta map[string]string
}

// Error returns the string representation of the error.
func (e *Error) Error() string { return e.msg }

// CursorPaginatedResponse provides a generic means of wrapping a paginated
// response.
type CursorPaginatedResponse[T any] struct {
	Data    []T  `json:"data"`
	HasNext bool `json:"has_next"`
	HasPrev bool `json:"has_prev"`
	// CursorStart is the ending cursor of the previous page.
	CursorStart *string `json:"cursor_start"`

	// CursorEnd is the starting cursor of the next page.
	CursorEnd *string `json:"cursor_end"`
}

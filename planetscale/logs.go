package planetscale

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"
)

const (
	logsBaseURL           = "logs.psdb.cloud"
	logsSignaturesAPIPath = "/logs/signatures"
)

type LogsService interface {
	Get(ctx context.Context, getReq *GetLogsRequest) (string, error)
	GetSignature(ctx context.Context, getReq *GetLogsSignatureRequest) (*Signature, error)
}

type logsService struct {
	client *Client
}

// GetLogsRequest encapsulates the request for resetting the default role of a Postgres database branch.
type GetLogsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`

	Replica bool      `json:"-"`
	Start   time.Time `json:"-"`
	End     time.Time `json:"-"`
	Level   []string  `json:"-"`
	Limit   int       `json:"-"`
}

type GetLogsSignatureRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

type Signature struct {
	Exp string `json:"exp"`
	Sig string `json:"sig"`
}

var _ LogsService = &logsService{}

func (l *logsService) GetSignature(ctx context.Context, getSigReq *GetLogsSignatureRequest) (*Signature, error) {
	path := path.Join(databaseBranchAPIPath(getSigReq.Organization, getSigReq.Database, getSigReq.Branch), logsSignaturesAPIPath)
	req, err := l.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for list regions: %w", err)
	}

	var signature Signature
	if err := l.client.do(ctx, req, &signature); err != nil {
		return nil, err
	}
	return &signature, nil
}

func (l *logsService) Get(ctx context.Context, getReq *GetLogsRequest) (string, error) {
	if getReq.End.Compare(getReq.Start) < 0 {
		return "", errors.New("end time cannot be before start time")
	}
	getReq.Limit = min(max(getReq.Limit, 0), 1000) // TODO reasonable?

	gbr := GetLogsSignatureRequest{Organization: getReq.Organization, Database: getReq.Database, Branch: getReq.Branch}
	sig, err := l.GetSignature(ctx, &gbr)
	if err != nil {
		fmt.Println("Signature error")
		return "", err
	}

	query := "* "

	// Time range
	query = fmt.Sprintf(query+"_time:[%s, %s] ", getReq.Start.Format(time.RFC3339), getReq.End.Format(time.RFC3339))

	// Log levels
	logLevels := map[string]struct{}{}
	for _, v := range getReq.Level {
		V := strings.ToUpper(v)
		switch V {
		case "DEBUG", "INFO", "WARNING", "ERROR":
			logLevels[V] = struct{}{}
		default:
			continue
		}
	}
	keys := make([]string, 0, len(logLevels))
	for k := range logLevels {
		keys = append(keys, fmt.Sprintf("planetscale.level:%s", k))
	}
	if len(keys) >= 1 {
		query = fmt.Sprintf(query+"(%s) ", strings.Join(keys, " OR "))
	}

	// Replica / Primary
	target := "primary"
	if getReq.Replica {
		target = "replica"
	}
	query = fmt.Sprintf(query+"(planetscale.role:%s) ", target)

	// Sort // TODO should this be more flexible?
	query = query + "| sort by (_time desc) | offset 0"

	u := &url.URL{
		Scheme: "https",
		Host:   logsBaseURL,
		Path:   fmt.Sprintf("/logs/branch/%s/query", getReq.Branch),
	}

	// Manage query parameters
	q := u.Query()
	q.Set("sig", sig.Sig)
	q.Set("exp", sig.Exp)
	q.Set("limit", fmt.Sprintf("%d", getReq.Limit))
	q.Set("query", query)
	u.RawQuery = q.Encode()
	fmt.Println(u.String())

	req, err := l.client.newRequest(http.MethodGet, u.String(), nil)
	if err != nil {
		return "", fmt.Errorf("error creating request for list regions: %w", err)
	}

	req = req.WithContext(ctx)
	res, err := l.client.client.Do(req)
	if err != nil {
		return "", err
	}
	defer res.Body.Close()
	out, err := io.ReadAll(res.Body)
	if err != nil {
		return "", err
	}
	if res.StatusCode != 200 {
		return "", fmt.Errorf("server responded with [%d]: %s", res.StatusCode, out)
	}

	return string(out), nil
}


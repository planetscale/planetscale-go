package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"path"
)

// TabletGroup represents a group of tablets in a keyspace/shard.
type TabletGroup struct {
	Type     string   `json:"type"`
	Keyspace string   `json:"keyspace"`
	Shard    string   `json:"shard"`
	Tablets  []Tablet `json:"tablets"`
}

// Tablet represents an individual vttablet.
type Tablet struct {
	Alias string `json:"alias"`
	Role  string `json:"role"`
	Cell  string `json:"cell"`
}

// ListBranchTabletsRequest is a request for listing tablets on a branch.
type ListBranchTabletsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
	Branch       string `json:"-"`
}

func tabletsAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "tablets")
}

func (s *vtctldService) ListTablets(ctx context.Context, req *ListBranchTabletsRequest) ([]*TabletGroup, error) {
	p := tabletsAPIPath(req.Organization, req.Database, req.Branch)
	httpReq, err := s.client.newRequest(http.MethodGet, p, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	var resp []*TabletGroup
	if err := s.client.do(ctx, httpReq, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

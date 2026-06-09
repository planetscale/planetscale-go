package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
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

	// The following are optional filters that mirror vtctldclient GetTablets.

	// Keyspace, if set, only returns tablets in this keyspace.
	Keyspace string `json:"-"`
	// Shard, if set, only returns tablets in this shard. It requires Keyspace
	// to also be set.
	Shard string `json:"-"`
	// TabletType, if set, only returns tablets of this type (e.g. "primary",
	// "replica", "rdonly").
	TabletType string `json:"-"`
	// TabletAliases, if set, only returns the tablets with these aliases (e.g.
	// "zone1-0000000100"). When set, the other filters are ignored.
	TabletAliases []string `json:"-"`
}

func tabletsAPIPath(org, db, branch string) string {
	return path.Join(databaseBranchAPIPath(org, db, branch), "tablets")
}

func (s *vtctldService) ListTablets(ctx context.Context, req *ListBranchTabletsRequest) ([]*TabletGroup, error) {
	p := tabletsAPIPath(req.Organization, req.Database, req.Branch)

	v := url.Values{}
	if req.Keyspace != "" {
		v.Set("keyspace", req.Keyspace)
	}
	if req.Shard != "" {
		v.Set("shard", req.Shard)
	}
	if req.TabletType != "" {
		v.Set("tablet_type", req.TabletType)
	}
	if len(req.TabletAliases) > 0 {
		v.Set("tablet_alias", strings.Join(req.TabletAliases, ","))
	}

	httpReq, err := s.client.newRequest(http.MethodGet, p, nil, WithQueryParams(v))
	if err != nil {
		return nil, fmt.Errorf("error creating http request: %w", err)
	}

	var resp []*TabletGroup
	if err := s.client.do(ctx, httpReq, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

package planetscale

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"path"
	"time"
)

// BranchInfrastructure represents the infrastructure for a branch. Exactly one
// of Vitess or Postgres is set, depending on the branch's database engine.
type BranchInfrastructure struct {
	Type     string
	Vitess   *VitessBranchInfrastructure
	Postgres *PostgresBranchInfrastructure
}

func (b *BranchInfrastructure) UnmarshalJSON(data []byte) error {
	var discriminator struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(data, &discriminator); err != nil {
		return err
	}
	b.Type = discriminator.Type

	switch b.Type {
	case "PostgresInfrastructure":
		b.Postgres = &PostgresBranchInfrastructure{}
		return json.Unmarshal(data, b.Postgres)
	default:
		b.Vitess = &VitessBranchInfrastructure{}
		return json.Unmarshal(data, b.Vitess)
	}
}

// VitessBranchInfrastructure represents the infrastructure (pods) for a Vitess
// branch.
type VitessBranchInfrastructure struct {
	Ready bool              `json:"ready"`
	Pods  []*BranchInfraPod `json:"pods"`
}

// PostgresBranchInfrastructure represents the infrastructure (nodes and
// bouncers) for a Postgres branch.
type PostgresBranchInfrastructure struct {
	State                           string                  `json:"state"`
	PrimaryName                     string                  `json:"primary_name"`
	PrimaryPromotedAt               *time.Time              `json:"primary_promoted_at"`
	VolumeModificationsBlockedUntil *time.Time              `json:"volume_modifications_blocked_until"`
	Nodes                           []*PostgresInfraNode    `json:"nodes"`
	Bouncers                        []*PostgresInfraBouncer `json:"bouncers"`
}

// BranchInfraPod represents a single pod in the branch infrastructure.
type BranchInfraPod struct {
	Name         string     `json:"name"`
	Status       string     `json:"status"`
	Component    string     `json:"component"`
	Ready        string     `json:"ready"`
	RestartCount int        `json:"restart_count"`
	CreatedAt    *time.Time `json:"created_at"`
	Cell         string     `json:"cell"`
	Size         string     `json:"size"`
	Keyspace     *string    `json:"keyspace"`
	Shard        *string    `json:"shard"`
	TabletType   *string    `json:"tablet_type"`
}

// PostgresInfraNode represents a single Postgres instance in the branch
// infrastructure.
type PostgresInfraNode struct {
	Name                         string                        `json:"name"`
	NormalizedName               string                        `json:"normalized_name"`
	Role                         string                        `json:"role"`
	AvailabilityZone             string                        `json:"availability_zone"`
	ClusterName                  string                        `json:"cluster_name"`
	ClusterDisplayName           string                        `json:"cluster_display_name"`
	PeersCount                   int                           `json:"peers_count"`
	VolumeUsageBytes             *int64                        `json:"volume_usage_bytes"`
	VolumeCapacityBytes          *int64                        `json:"volume_capacity_bytes"`
	VolumeShrinkThresholdPercent *float64                      `json:"volume_shrink_threshold_percent"`
	Region                       Region                        `json:"region"`
	DiskReplacement              *PostgresInfraDiskReplacement `json:"disk_replacement"`
}

// PostgresInfraDiskReplacement represents a scheduled disk replacement for a
// Postgres node.
type PostgresInfraDiskReplacement struct {
	Reason      string     `json:"reason"`
	Bytes       int64      `json:"bytes"`
	ScheduledAt *time.Time `json:"scheduled_at"`
}

// PostgresInfraBouncer represents a PgBouncer deployment in the branch
// infrastructure.
type PostgresInfraBouncer struct {
	ID              string                   `json:"id"`
	Name            string                   `json:"name"`
	Target          string                   `json:"target"`
	ReplicasPerCell int                      `json:"replicas_per_cell"`
	Region          Region                   `json:"region"`
	SKU             *PostgresInfraBouncerSKU `json:"sku"`
}

// PostgresInfraBouncerSKU represents the size of a PgBouncer deployment.
type PostgresInfraBouncerSKU struct {
	Name        string `json:"name"`
	DisplayName string `json:"display_name"`
	CPU         string `json:"cpu"`
	RAM         int64  `json:"ram"`
}

// GetBranchInfrastructureRequest encapsulates the request for getting branch infrastructure.
type GetBranchInfrastructureRequest struct {
	Organization string
	Database     string
	Branch       string
}

// BranchInfrastructureService is an interface for interacting with the branch infrastructure API.
type BranchInfrastructureService interface {
	Get(ctx context.Context, req *GetBranchInfrastructureRequest) (*BranchInfrastructure, error)
}

type branchInfrastructureService struct {
	client *Client
}

var _ BranchInfrastructureService = &branchInfrastructureService{}

func NewBranchInfrastructureService(client *Client) *branchInfrastructureService {
	return &branchInfrastructureService{
		client: client,
	}
}

func (s *branchInfrastructureService) Get(ctx context.Context, getReq *GetBranchInfrastructureRequest) (*BranchInfrastructure, error) {
	p := path.Join(
		databaseBranchAPIPath(getReq.Organization, getReq.Database, getReq.Branch),
		"infrastructure",
	)

	req, err := s.client.newRequest(http.MethodGet, p, nil)
	if err != nil {
		return nil, fmt.Errorf("error creating request for get branch infrastructure: %w", err)
	}

	infra := &BranchInfrastructure{}
	if err := s.client.do(ctx, req, &infra); err != nil {
		return nil, err
	}

	return infra, nil
}

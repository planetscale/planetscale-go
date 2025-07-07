package planetscale

import (
	"context"
)

type PostgresBranch struct {
	ID                  string `json:"id"`
	Name                string `json:"name"`
	ClusterName         string `json:"cluster_name"`
	ClusterDisplayName  string `json:"cluster_display_name"`
	ClusterArchitecture string `json:"cluster_architecture"`
}

type CreatePostgresBranchRequest struct{}

type ListPostgresBranchesRequest struct{}

type GetPostgresBranchRequest struct{}

type DeletePostgresBranchRequest struct{}

type PostgresBranchSchemaRequest struct{}

type ListPostgresBranchClusterSKUsRequest struct{}

// PostgresBranchSchema encapsulates the schema of a Postgres branch.
type PostgresBranchSchema struct {
	Name string `json:"name"`
	Raw  string `json:"raw"`
	HTML string `json:"html"`
}

type PostgresBranchesService interface {
	Create(context.Context, *CreatePostgresBranchRequest) (*PostgresBranch, error)
	List(context.Context, *ListPostgresBranchesRequest) ([]*PostgresBranch, error)
	Get(context.Context, *GetPostgresBranchRequest) (*PostgresBranch, error)
	Delete(context.Context, *DeletePostgresBranchRequest) error
	Schema(context.Context, *PostgresBranchSchemaRequest) (*PostgresBranchSchema, error)
	ListClusterSKUs(context.Context, *ListBranchClusterSKUsRequest) ([]*ClusterSKU, error)
}

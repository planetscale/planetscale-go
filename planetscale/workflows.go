package planetscale

import (
	"time"
)

type Workflow struct {
	PublicID             string     `json:"id"`
	Name                 string     `json:"name"`
	Number               int        `json:"number"`
	State                string     `json:"state"`
	CreatedAt            time.Time  `json:"created_at"`
	UpdatedAt            time.Time  `json:"updated_at"`
	StartedAt            *time.Time `json:"started_at"`
	CompletedAt          *time.Time `json:"completed_at"`
	CancelledAt          *time.Time `json:"cancelled_at"`
	ReversedAt           *time.Time `json:"reversed_at"`
	RetriedAt            *time.Time `json:"retried_at"`
	DataCopycCompletedAt *time.Time `json:"data_copy_completed_at"`
	CutoverAt            *time.Time `json:"cutover_at"`
	ReplicasSwitched     bool       `json:"replicas_switched"`
	PrimariesSwitched    bool       `json:"primaries_switched"`
	SwitchReplicasAt     *time.Time `json:"switch_replicas_at"`
	SwitchPrimariesAt    *time.Time `json:"switch_primaries_at"`
	VerifyDataAt         *time.Time `json:"verify_data_at"`

	Branch         DatabaseBranch `json:"database_branch"`
	SourceKeyspace Keyspace       `json:"source_keyspace"`
	TargetKeyspace Keyspace       `json:"target_keyspace"`

	Actor             Actor  `json:"actor"`
	VerifyDataBy      *Actor `json:"verify_data_by"`
	ReversedBy        *Actor `json:"reversed_by"`
	SwitchReplicasBy  *Actor `json:"switch_replicas_by"`
	SwitchPrimariesBy *Actor `json:"switch_primaries_by"`
	CancelledBy       *Actor `json:"cancelled_by"`
	CompletedBy       *Actor `json:"completed_by"`
	RetriedBy         *Actor `json:"retried_by"`
	CutoverBy         *Actor `json:"cutover_by"`
	ReversedCutoverBy *Actor `json:"reversed_cutover_by"`

	Streams []WorkflowStream `json:"streams"`
	Tables  []WorkflowTable  `json:"tables"`
	VDiff   WorkflowVDiff    `json:"vdiff"`
}

type WorkflowStream struct {
	PublicID             string              `json:"id"`
	State                string              `json:"state"`
	CreatedAt            time.Time           `json:"created_at"`
	UpdatedAt            time.Time           `json:"updated_at"`
	TargetShard          string              `json:"target_shard"`
	SourceShard          string              `json:"source_shard"`
	Position             string              `json:"position"`
	StopPosition         string              `json:"stop_position"`
	RowsCopied           string              `json:"rows_copied"`
	ComponentThrottled   *string             `json:"component_throttled"`
	ComponentThrottledAt *time.Time          `json:"component_throttled_at"`
	PrimaryServing       bool                `json:"primary_serving"`
	Info                 string              `json:"info"`
	Logs                 []WorkflowStreamLog `json:"logs"`
}

type WorkflowStreamLog struct {
	PublicID  string    `json:"id"`
	State     string    `json:"state"`
	CreatedAt time.Time `json:"created_at"`
	UpdatedAt time.Time `json:"updated_at"`
	Message   string    `json:"message"`
	LogType   string    `json:"log_type"`
}

type WorkflowTable struct {
	PublicID       string    `json:"id"`
	Name           string    `json:"name"`
	CreatedAt      time.Time `json:"created_at"`
	UpdatedAt      time.Time `json:"updated_at"`
	RowsCopied     int64     `json:"rows_copied"`
	RowsTotal      int64     `json:"rows_total"`
	RowsPercentage int       `json:"rows_percentage"`
}

type WorkflowVDiff struct {
	PublicID           string                     `json:"id"`
	State              string                     `json:"state"`
	CreatedAt          time.Time                  `json:"created_at"`
	UpdatedAt          time.Time                  `json:"updated_at"`
	StartedAt          *time.Time                 `json:"started_at"`
	CompletedAt        *time.Time                 `json:"completed_at"`
	HasMismatch        bool                       `json:"has_mismatch"`
	ProgressPercentage int                        `json:"progress_percentage"`
	EtaSeconds         int64                      `json:"eta_seconds"`
	TableReports       []WorkflowVDiffTableReport `json:"table_reports"`
}

type WorkflowVDiffTableReport struct {
	PublicID                   string                 `json:"id"`
	TableName                  string                 `json:"table_name"`
	Shard                      string                 `json:"shard"`
	MismatchedRowsCount        int64                  `json:"mismatched_rows_count"`
	ExtraSourceRowsCount       int64                  `json:"extra_source_rows_count"`
	ExtraTargetRowsCount       int64                  `json:"extra_target_rows_count"`
	ExtraSourceRows            map[string]interface{} `json:"extra_source_rows"`
	ExtraTargetRows            map[string]interface{} `json:"extra_target_rows"`
	MismatchedRows             map[string]interface{} `json:"mismatched_rows"`
	SampleExtraSourceRowsQuery string                 `json:"sample_extra_source_rows_query"`
	SampleExtraTargetRowsQuery string                 `json:"sample_extra_target_rows_query"`
	SampleMismatchedRowsQuery  string                 `json:"sample_mismatched_rows_query"`
	CreatedAt                  time.Time              `json:"created_at"`
	UpdatedAt                  time.Time              `json:"updated_at"`
}

// WorkflowsService is an interface for interacting with the keyspace endpoints of the PlanetScale API
type WorkflowsService interface {
	// Create(context.Context, *CreateKeyspaceRequest) (*Keyspace, error)
	// List(context.Context, *ListKeyspacesRequest) ([]*Keyspace, error)
	// Get(context.Context, *GetKeyspaceRequest) (*Keyspace, error)
	// VSchema(context.Context, *GetKeyspaceVSchemaRequest) (*VSchema, error)
	// UpdateVSchema(context.Context, *UpdateKeyspaceVSchemaRequest) (*VSchema, error)
	// Resize(context.Context, *ResizeKeyspaceRequest) (*KeyspaceResizeRequest, error)
	// CancelResize(context.Context, *CancelKeyspaceResizeRequest) error
	// ResizeStatus(context.Context, *KeyspaceResizeStatusRequest) (*KeyspaceResizeRequest, error)
	// RolloutStatus(context.Context, *KeyspaceRolloutStatusRequest) (*KeyspaceRollout, error)
}

type workflowsService struct {
	client *Client
}

var _ WorkflowsService = &workflowsService{}

func NeWorkflowsService(client *Client) *workflowsService {
	return &workflowsService{client}
}

package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type Workflow struct {
	ID                  string     `json:"id"`
	Name                string     `json:"name"`
	Number              uint64     `json:"number"`
	State               string     `json:"state"`
	CreatedAt           time.Time  `json:"created_at"`
	UpdatedAt           time.Time  `json:"updated_at"`
	StartedAt           *time.Time `json:"started_at"`
	CompletedAt         *time.Time `json:"completed_at"`
	CancelledAt         *time.Time `json:"cancelled_at"`
	ReversedAt          *time.Time `json:"reversed_at"`
	RetriedAt           *time.Time `json:"retried_at"`
	DataCopyCompletedAt *time.Time `json:"data_copy_completed_at"`
	CutoverAt           *time.Time `json:"cutover_at"`
	ReplicasSwitched    bool       `json:"replicas_switched"`
	PrimariesSwitched   bool       `json:"primaries_switched"`
	SwitchReplicasAt    *time.Time `json:"switch_replicas_at"`
	SwitchPrimariesAt   *time.Time `json:"switch_primaries_at"`
	VerifyDataAt        *time.Time `json:"verify_data_at"`

	Branch         DatabaseBranch `json:"branch"`
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

	Streams []*WorkflowStream `json:"streams"`
	Tables  []*WorkflowTable  `json:"tables"`
	VDiff   *WorkflowVDiff    `json:"vdiff"`
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
	RowsCopied           int64               `json:"rows_copied"`
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
	RowsCopied     uint64    `json:"rows_copied"`
	RowsTotal      uint64    `json:"rows_total"`
	RowsPercentage uint      `json:"rows_percentage"`
}

type WorkflowVDiff struct {
	PublicID           string                     `json:"id"`
	State              string                     `json:"state"`
	CreatedAt          time.Time                  `json:"created_at"`
	UpdatedAt          time.Time                  `json:"updated_at"`
	StartedAt          *time.Time                 `json:"started_at"`
	CompletedAt        *time.Time                 `json:"completed_at"`
	HasMismatch        bool                       `json:"has_mismatch"`
	ProgressPercentage uint                       `json:"progress_percentage"`
	EtaSeconds         uint64                     `json:"eta_seconds"`
	TableReports       []WorkflowVDiffTableReport `json:"table_reports"`
}

type WorkflowVDiffTableReport struct {
	PublicID                   string        `json:"id"`
	TableName                  string        `json:"table_name"`
	Shard                      string        `json:"shard"`
	MismatchedRowsCount        int64         `json:"mismatched_rows_count"`
	ExtraSourceRowsCount       int64         `json:"extra_source_rows_count"`
	ExtraTargetRowsCount       int64         `json:"extra_target_rows_count"`
	ExtraSourceRows            []interface{} `json:"extra_source_rows"`
	ExtraTargetRows            []interface{} `json:"extra_target_rows"`
	MismatchedRows             []interface{} `json:"mismatched_rows"`
	SampleExtraSourceRowsQuery string        `json:"sample_extra_source_rows_query"`
	SampleExtraTargetRowsQuery string        `json:"sample_extra_target_rows_query"`
	SampleMismatchedRowsQuery  string        `json:"sample_mismatched_rows_query"`
	CreatedAt                  time.Time     `json:"created_at"`
	UpdatedAt                  time.Time     `json:"updated_at"`
}

type ListWorkflowsRequest struct {
	Organization string `json:"-"`
	Database     string `json:"-"`
}

type GetWorkflowRequest struct {
	Organization   string `json:"-"`
	Database       string `json:"-"`
	WorkflowNumber uint64 `json:"-"`
}

type CreateWorkflowRequest struct {
	Organization       string   `json:"-"`
	Database           string   `json:"-"`
	Branch             string   `json:"branch_name"`
	Name               string   `json:"name"`
	SourceKeyspace     string   `json:"source_keyspace"`
	TargetKeyspace     string   `json:"target_keyspace"`
	Tables             []string `json:"tables"`
	GlobalKeyspace     *string  `json:"global_keyspace"`
	DeferSecondaryKeys *bool    `json:"defer_secondary_keys"`
	OnDDL              *string  `json:"on_ddl"`
}

// WorkflowsService is an interface for interacting with the workflow endpoints of the PlanetScale API
type WorkflowsService interface {
	List(context.Context, *ListWorkflowsRequest) ([]*Workflow, error)
	Get(context.Context, *GetWorkflowRequest) (*Workflow, error)
	Create(context.Context, *CreateWorkflowRequest) (*Workflow, error)
	VerifyData(context.Context, *GetWorkflowRequest) (*Workflow, error)
}

type workflowsService struct {
	client *Client
}

var _ WorkflowsService = &workflowsService{}

func NewWorkflowsService(client *Client) *workflowsService {
	return &workflowsService{client}
}

type workflowsResponse struct {
	Workflows []*Workflow `json:"data"`
}

func (ws *workflowsService) List(ctx context.Context, listReq *ListWorkflowsRequest) ([]*Workflow, error) {
	req, err := ws.client.newRequest(http.MethodGet, workflowsAPIPath(listReq.Organization, listReq.Database), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	workflows := &workflowsResponse{}

	if err := ws.client.do(ctx, req, workflows); err != nil {
		return nil, err
	}

	return workflows.Workflows, nil
}

func (ws *workflowsService) Get(ctx context.Context, getReq *GetWorkflowRequest) (*Workflow, error) {
	req, err := ws.client.newRequest(http.MethodGet, workflowAPIPath(getReq.Organization, getReq.Database, getReq.WorkflowNumber), nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	workflow := &Workflow{}

	if err := ws.client.do(ctx, req, workflow); err != nil {
		return nil, err
	}

	return workflow, nil
}

func (ws *workflowsService) Create(ctx context.Context, createReq *CreateWorkflowRequest) (*Workflow, error) {
	req, err := ws.client.newRequest(http.MethodPost, workflowsAPIPath(createReq.Organization, createReq.Database), createReq)

	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	workflow := &Workflow{}

	if err := ws.client.do(ctx, req, workflow); err != nil {
		return nil, err
	}

	return workflow, nil
}

func (ws *workflowsService) VerifyData(ctx context.Context, verifyDataReq *GetWorkflowRequest) (*Workflow, error) {
	path := fmt.Sprintf("%s/verify_data", workflowAPIPath(verifyDataReq.Organization, verifyDataReq.Database, verifyDataReq.WorkflowNumber))
	req, err := ws.client.newRequest(http.MethodPatch, path, nil)

	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	workflow := &Workflow{}

	if err := ws.client.do(ctx, req, workflow); err != nil {
		return nil, err
	}

	return workflow, nil
}

func workflowsAPIPath(org, db string) string {
	return fmt.Sprintf("%s/%s/workflows", databasesAPIPath(org), db)
}

func workflowAPIPath(org, db string, number uint64) string {
	return fmt.Sprintf("%s/%d", workflowsAPIPath(org, db), number)
}

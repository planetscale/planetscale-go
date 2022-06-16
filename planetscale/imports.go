package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/pkg/errors"
)

type BillingPlan int

const (
	HobbyPlan BillingPlan = iota
	ScalerPlan
)

func (bp BillingPlan) String() string {
	switch bp {
	case ScalerPlan:
		return "scaler"
	default:
		return "developer"
	}
}

var planToBillingPlanMap = map[string]BillingPlan{
	"scaler":    ScalerPlan,
	"developer": HobbyPlan,
}

type DataImportSource struct {
	HostName            string `json:"hostname"`
	Database            string `json:"schema_name"`
	Port                int    `json:"port"`
	SSLMode             string `json:"ssl_mode"`
	SSLVerificationMode ExternalDataSourceSSLVerificationMode
	UserName            string `json:"username"`
	Password            string `json:"password"`
}

type ExternalDataSourceSSLVerificationMode int

const (
	SSLModeDisabled ExternalDataSourceSSLVerificationMode = iota
	SSLModePreferred
	SSLModeRequired
	SSLModeVerifyCA
	SSLModeVerifyIdentity
)

func (sm ExternalDataSourceSSLVerificationMode) String() string {
	switch sm {
	case SSLModeDisabled:
		return "disabled"
	case SSLModePreferred:
		return "preferred"
	case SSLModeRequired:
		return "required"
	case SSLModeVerifyCA:
		return "verify_ca"
	default:
		return "verify_identity"
	}
}

type DataImportState int

const (
	DataImportPreparingDataCopy DataImportState = iota
	DataImportPreparingDataCopyFailed
	DataImportCopyingData
	DataImportCopyingDataFailed
	DataImportSwitchTrafficPending
	DataImportSwitchTrafficRunning
	DataImportSwitchTrafficCompleted
	DataImportSwitchTrafficError
	DataImportReverseTrafficRunning
	DataImportReverseTrafficCompleted
	DataImportReverseTrafficError
	DataImportDetachExternalDatabaseRunning
	DataImportDetachExternalDatabaseError
	DataImportReady
)

var stateToImportStateMap = map[string]DataImportState{
	"prepare_data_copy_pending":        DataImportPreparingDataCopy,
	"prepare_data_copy_error":          DataImportPreparingDataCopyFailed,
	"data_copy_pending":                DataImportCopyingData,
	"data_copy_error":                  DataImportCopyingDataFailed,
	"switch_traffic_workflow_pending":  DataImportSwitchTrafficPending,
	"switch_traffic_workflow_running":  DataImportSwitchTrafficRunning,
	"switch_traffic_workflow_error":    DataImportSwitchTrafficError,
	"reverse_traffic_workflow_running": DataImportReverseTrafficRunning,
	"reverse_traffic_workflow_error":   DataImportReverseTrafficError,
	"cleanup_workflow_pending":         DataImportSwitchTrafficCompleted,
	"cleanup_workflow_running":         DataImportDetachExternalDatabaseRunning,
	"cleanup_workflow_error":           DataImportDetachExternalDatabaseError,
	"ready":                            DataImportReady,
}

var importStateToDescMap = map[DataImportState]string{
	DataImportPreparingDataCopy:             "Preparing to copy data from external database",
	DataImportPreparingDataCopyFailed:       "Failed to copy data from external database",
	DataImportCopyingData:                   "Copying data from external database",
	DataImportCopyingDataFailed:             "Failed to copy data from external database",
	DataImportSwitchTrafficPending:          "PlanetScale database is running in replica mode",
	DataImportSwitchTrafficRunning:          "Switching PlanetScale database to primary mode",
	DataImportSwitchTrafficError:            "Failed to switching PlanetScale database to primary mode",
	DataImportReverseTrafficRunning:         "Switching PlanetScale database to replica mode",
	DataImportReverseTrafficError:           "Failed to switching PlanetScale database to replica mode",
	DataImportDetachExternalDatabaseRunning: "Detaching external database from  PlanetScale database",
	DataImportDetachExternalDatabaseError:   "Failed to detach external database from  PlanetScale database",
	DataImportReady:                         "Import has completed and your PlanetScale Database is now ready",
}

func (d DataImportState) String() string {
	if val, ok := importStateToDescMap[d]; ok {
		return val
	}

	panic("unknown data import state")
}

type DataImport struct {
	ID                 string `json:"id"`
	ImportState        DataImportState
	State              string           `json:"state"`
	Errors             string           `json:"import_check_errors"`
	StartedAt          *time.Time       `json:"started_at"`
	FinishedAt         *time.Time       `json:"finished_at"`
	DeletedAt          *time.Time       `json:"deleted_at"`
	ExternalDataSource DataImportSource `json:"data_source"`
}

func (di *DataImport) ParseState() {
	if val, ok := stateToImportStateMap[di.State]; ok {
		di.ImportState = val
		return
	}

	panic("unknown data import state " + di.State)
}

type TestDataImportSourceRequest struct {
	Organization string           `json:"organization"`
	Database     string           `json:"database_name"`
	Connection   DataImportSource `json:"connection"`
}

// DataSourceIncompatibilityError represents an error that occurs when the
// source schema in an external database server is incompatible with PlanetScale.
type DataSourceIncompatibilityError struct {
	LintError        string `json:"lint_error"`
	Keyspace         string `json:"keyspace_name"`
	Table            string `json:"table_name"`
	SubjectType      string `json:"subject_type"`
	ErrorDescription string `json:"error_description"`
	DocsUrl          string `json:"docs_url"`
}

type UserShouldUpgradePlanError struct{}

func (e UserShouldUpgradePlanError) Error() string {
	return "Importing databases over 5GB requires a paid plan. Log in to app.planetscale.com to upgrade."
}

type TestDataImportSourceResponse struct {
	CanConnect           bool   `json:"can_connect"`
	ShouldUpgradePlan    bool   `json:"should_upgrade"`
	SuggestedPlan        string `json:"suggested_plan"`
	SuggestedBillingPlan BillingPlan
	ConnectError         string                            `json:"error"`
	Errors               []*DataSourceIncompatibilityError `json:"lint_errors"`
}

type StartDataImportRequest struct {
	Database     string           `json:"database_name"`
	Organization string           `json:"organization"`
	Connection   DataImportSource `json:"connection"`
	Plan         string           `json:"plan"`
}

type MakePlanetScalePrimaryRequest struct {
	Organization string
	Database     string
}

type MakePlanetScaleReplicaRequest struct {
	Organization string
	Database     string
}

type DetachExternalDatabaseRequest struct {
	Organization string
	Database     string
}

type GetImportStatusRequest struct {
	Organization string
	Database     string
}

type CancelDataImportRequest struct {
	Organization string
	Database     string
}

// DataImportsService is an interface for communicating with the PlanetScale
// Data Imports API endpoint.
type DataImportsService interface {
	// TestDataImportSource checks if the external database that we're importing will be supported
	// by PlanetScale. It checks for ability to replicate binlogs, schema compatibility and other factors.
	TestDataImportSource(ctx context.Context, request *TestDataImportSourceRequest) (*TestDataImportSourceResponse, error)
	// StartDataImport spins up a downstream PlanetScale database in replica mode, with the
	// external database as a Primary and starts copying data from external to PlanetScale.
	StartDataImport(ctx context.Context, request *StartDataImportRequest) (*DataImport, error)
	// CancelDataImport halts all replication and data copy from external to PlanetScale
	// and deletes the PlanetScale database.
	CancelDataImport(ctx context.Context, request *CancelDataImportRequest) error
	// GetDataImportStatus gets the current status of a DataImport for a given database
	// Fails if the database is not importing any data.
	GetDataImportStatus(ctx context.Context, request *GetImportStatusRequest) (*DataImport, error)
	// MakePlanetScalePrimary makes the downstream PlanetScale database a Primary and the external database a Replica.
	MakePlanetScalePrimary(ctx context.Context, request *MakePlanetScalePrimaryRequest) (*DataImport, error)
	// MakePlanetScaleReplica makes the downstream PlanetScale database a Replica and the external database a Primary.
	MakePlanetScaleReplica(ctx context.Context, request *MakePlanetScaleReplicaRequest) (*DataImport, error)
	// DetachExternalDatabase detaches the external database from PlanetScale after a data import has finished
	// and PlanetScale is running as Primary.
	DetachExternalDatabase(ctx context.Context, request *DetachExternalDatabaseRequest) (*DataImport, error)
}

type dataImportsService struct {
	client *Client
}

// TestDataImportSource will check an external database for compatibility with PlanetScale
func (d *dataImportsService) TestDataImportSource(ctx context.Context, request *TestDataImportSourceRequest) (*TestDataImportSourceResponse, error) {
	request.Connection.SSLMode = request.Connection.SSLVerificationMode.String()
	path := fmt.Sprintf("/v1/organizations/%s/data-imports/test-connection", request.Organization)
	req, err := d.client.newRequest(http.MethodPost, path, request)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &TestDataImportSourceResponse{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	if resp.ShouldUpgradePlan {
		return resp, UserShouldUpgradePlanError{}
	}

	resp.SuggestedBillingPlan = planToBillingPlanMap[resp.SuggestedPlan]
	return resp, nil
}

func (d *dataImportsService) StartDataImport(ctx context.Context, request *StartDataImportRequest) (*DataImport, error) {
	request.Connection.SSLMode = request.Connection.SSLVerificationMode.String()
	path := fmt.Sprintf("/v1/organizations/%s/data-imports/new", request.Organization)
	req, err := d.client.newRequest(http.MethodPost, path, request)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &DataImport{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (d *dataImportsService) GetDataImportStatus(ctx context.Context, getReq *GetImportStatusRequest) (*DataImport, error) {
	path := dataImportAPIPath(getReq.Organization, getReq.Database)
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for get database")
	}

	di := &DataImport{}
	err = d.client.do(ctx, req, &di)
	if err != nil {
		return nil, err
	}

	di.ParseState()
	return di, nil
}

func (d *dataImportsService) CancelDataImport(ctx context.Context, cancelReq *CancelDataImportRequest) error {
	path := fmt.Sprintf("%s/cancel", dataImportAPIPath(cancelReq.Organization, cancelReq.Database))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return errors.Wrap(err, "error creating http request")
	}

	if err := d.client.do(ctx, req, nil); err != nil {
		return err
	}

	return nil
}
func (d *dataImportsService) MakePlanetScalePrimary(ctx context.Context, request *MakePlanetScalePrimaryRequest) (*DataImport, error) {
	path := fmt.Sprintf("%s/begin-switch-traffic", dataImportAPIPath(request.Organization, request.Database))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &DataImport{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}
	resp.ParseState()
	return resp, nil
}

func (d *dataImportsService) MakePlanetScaleReplica(ctx context.Context, request *MakePlanetScaleReplicaRequest) (*DataImport, error) {
	path := fmt.Sprintf("%s/begin-reverse-traffic", dataImportAPIPath(request.Organization, request.Database))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &DataImport{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}
	resp.ParseState()
	return resp, nil
}

func (d *dataImportsService) DetachExternalDatabase(ctx context.Context, request *DetachExternalDatabaseRequest) (*DataImport, error) {
	path := fmt.Sprintf("%s/detach-external-database", dataImportAPIPath(request.Organization, request.Database))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &DataImport{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	resp.ParseState()
	return resp, nil
}

func dataImportAPIPath(organization, database string) string {
	return fmt.Sprintf("/v1/organizations/%s/databases/%s/data-imports", organization, database)
}

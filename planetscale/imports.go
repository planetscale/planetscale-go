package planetscale

import (
	"context"
	"fmt"
	"github.com/pkg/errors"
	"net/http"
	"time"
)

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
	Ready
)

func (d DataImportState) String() string {
	switch d {
	case DataImportPreparingDataCopy:
		return "preparing to copy data from external database"
	case DataImportPreparingDataCopyFailed:
		return "failed to copy data from external database"
	case DataImportCopyingData:
		return "copying data from external database"
	case DataImportCopyingDataFailed:
		return "failed to copy data from external database"
	case DataImportSwitchTrafficPending:
		return "PlanetScale database is running in replica mode"
	case DataImportSwitchTrafficRunning:
		return "switching PlanetScale database to primary mode"
	case DataImportSwitchTrafficError:
		return "failed to switching PlanetScale database to primary mode"
	case DataImportReverseTrafficRunning:
		return "switching PlanetScale database to replica mode"
	case DataImportReverseTrafficError:
		return "failed to switching PlanetScale database to replica mode"
	case DataImportDetachExternalDatabaseRunning:
		return "detaching external database from  PlanetScale database"
	case DataImportDetachExternalDatabaseError:
		return "failed to detach external database from  PlanetScale database"
	}

	return "all done"
}

type DataImport struct {
	ID          string `json:"id"`
	ImportState DataImportState
	State       string    `json:"state"`
	Errors      string    `json:"import_check_errors"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	DeleteddAt  time.Time `json:"deleted_at"`
}

func (di *DataImport) ParseState() {
	switch di.State {
	case "prepare_data_copy_pending":
		di.ImportState = DataImportPreparingDataCopy
		break
	case "prepare_data_copy_error":
		di.ImportState = DataImportPreparingDataCopyFailed
		break
	case "data_copy_pending":
		di.ImportState = DataImportCopyingData
		break
	case "data_copy_error":
		di.ImportState = DataImportPreparingDataCopyFailed
		break
	case "switch_traffic_workflow_pending":
		di.ImportState = DataImportSwitchTrafficPending
		break
	case "switch_traffic_workflow_running":
		di.ImportState = DataImportSwitchTrafficRunning
		break
	case "switch_traffic_workflow_error":
		di.ImportState = DataImportSwitchTrafficError
		break
	case "reverse_traffic_workflow_running":
		di.ImportState = DataImportReverseTrafficRunning
		break
	case "reverse_traffic_workflow_error":
		di.ImportState = DataImportReverseTrafficError
		break
	case "cleanup_workflow_pending":
		di.ImportState = DataImportSwitchTrafficCompleted
		break
	case "cleanup_workflow_running":
		di.ImportState = DataImportDetachExternalDatabaseRunning
		break
	case "cleanup_workflow_error":
		di.ImportState = DataImportDetachExternalDatabaseError
		break
	default:
		di.ImportState = Ready
	}
}

type TestDataImportSourceRequest struct {
	Organization string           `json:"organization"`
	Database     string           `json:"name"`
	Source       DataImportSource `json:"connection"`
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

type TestDataImportSourceResponse struct {
	CanConnect   bool                              `json:"can_connect"`
	ConnectError string                            `json:"error"`
	Errors       []*DataSourceIncompatibilityError `json:"lint_errors"`
}

type StartDataImportRequest struct {
	DatabaseName string           `json:"name"`
	Organization string           `json:"organization"`
	Source       DataImportSource `json:"connection"`
}

type MakePlanetScalePrimaryRequest struct {
	Organization string `json:"organization"`
	Database     string `json:"name"`
}

type MakePlanetScaleReplicaRequest struct {
	Organization string `json:"organization"`
	Database     string `json:"name"`
}

type DetachExternalDatabaseRequest struct {
	Organization string `json:"organization"`
	Database     string `json:"name"`
}

type GetImportStatusRequest struct {
	Organization string `json:"organization"`
	Database     string `json:"name"`
}

type CancelDataImportRequest struct {
	Organization string `json:"organization"`
	Database     string `json:"name"`
}

// DataImportsService is an interface for communicating with the PlanetScale
// Data Imports API endpoint.
type DataImportsService interface {
	TestDataImportSource(ctx context.Context, request *TestDataImportSourceRequest) (*TestDataImportSourceResponse, error)
	StartDataImport(ctx context.Context, request *StartDataImportRequest) (*DataImport, error)
	CancelDataImport(ctx context.Context, request *CancelDataImportRequest) (*DataImport, error)
	GetDataImportStatus(ctx context.Context, request *GetImportStatusRequest) (*DataImport, error)
	MakePlanetScalePrimary(ctx context.Context, request *MakePlanetScalePrimaryRequest) (*DataImport, error)
	MakePlanetScaleReplica(ctx context.Context, request *MakePlanetScaleReplicaRequest) (*DataImport, error)
	DetachExternalDatabase(ctx context.Context, request *DetachExternalDatabaseRequest) (*DataImport, error)
}

type dataImportsService struct {
	client *Client
}

// TestDataImportSource will check an external database for compatibility with PlanetScale
func (d *dataImportsService) TestDataImportSource(ctx context.Context, request *TestDataImportSourceRequest) (*TestDataImportSourceResponse, error) {
	request.Source.SSLMode = request.Source.SSLVerificationMode.String()
	path := fmt.Sprintf("/v1/organizations/%s/data-imports/test-connection", request.Organization)
	req, err := d.client.newRequest(http.MethodPost, path, request)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &TestDataImportSourceResponse{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	return resp, nil
}

func (d *dataImportsService) StartDataImport(ctx context.Context, request *StartDataImportRequest) (*DataImport, error) {
	request.Source.SSLMode = request.Source.SSLVerificationMode.String()
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
	path := fmt.Sprintf("/v1/organizations/%s/databases/%s", getReq.Organization, getReq.Database)
	req, err := d.client.newRequest(http.MethodGet, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating request for get database")
	}

	db := &Database{}
	err = d.client.do(ctx, req, &db)
	if err != nil {
		return nil, err
	}

	if db.DataImport.ID == "" {
		return nil, errors.Errorf("Database %s is not importing data", getReq.Database)
	}

	db.DataImport.ParseState()
	return &db.DataImport, nil
}

func (d *dataImportsService) CancelDataImport(ctx context.Context, cancelReq *CancelDataImportRequest) (*DataImport, error) {
	path := fmt.Sprintf("%s/cancel", dataImportAPIPath(cancelReq.Organization, cancelReq.Database))
	req, err := d.client.newRequest(http.MethodPost, path, nil)
	if err != nil {
		return nil, errors.Wrap(err, "error creating http request")
	}

	resp := &DataImport{}
	if err := d.client.do(ctx, req, &resp); err != nil {
		return nil, err
	}

	return resp, nil
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
	path := fmt.Sprintf("%s/cleanup", dataImportAPIPath(request.Organization, request.Database))
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

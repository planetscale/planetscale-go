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
	Database            string `json:"database"`
	Port                int    `json:"port"`
	SSLVerificationMode string `json:"ssl_mode"`
	UserName            string `json:"username"`
	Password            string `json:"password"`
}

type DataImportState int

const (
	PreparingDataCopy DataImportState = iota
	PreparingDataCopyFailed
	CopyingData
	CopyingDataFailed
	SwitchTrafficPending
	SwitchTrafficRunning
	SwitchTrafficCompleted
	SwitchTrafficError
	ReverseTrafficRunning
	ReverseTrafficCompleted
	ReverseTrafficError
	DetachExternalDatabaseRunning
	DetachExternalDatabaseError
	Ready
)

type DataImport struct {
	ID          string `json:"id"`
	ImportState DataImportState
	Errors      string    `json:"import_check_errors"`
	StartedAt   time.Time `json:"started_at"`
	FinishedAt  time.Time `json:"finished_at"`
	DeleteddAt  time.Time `json:"deleted_at"`
}

type TestDataImportSourceRequest struct {
	Organization string           `json:"organization"`
	Source       DataImportSource `json:"connection"`
}

// PromotionRequestLintError represents an error that occurs during branch
// promotion.
type DataSourceIncompatiblityError struct {
	LintError        string `json:"lint_error"`
	Keyspace         string `json:"keyspace_name"`
	Table            string `json:"table_name"`
	SubjectType      string `json:"subject_type"`
	ErrorDescription string `json:"error_description"`
	DocsUrl          string `json:"docs_url"`
}

type TestDataImportSourceResponse struct {
	CanConnect   bool                             `json:"can_connect"`
	ConnectError string                           `json:"error"`
	Errors       []*DataSourceIncompatiblityError `json:"lint_errors"`
}

type CreateDataImportRequest struct {
	Organization string           `json:"organization"`
	DatabaseName string           `json:"name"`
	Source       DataImportSource `json:"connection"`
}

type MakePlanetScalePrimaryRequest struct {
	Organization string `json:"organization"`
	DatabaseName string `json:"name"`
}

type MakePlanetScaleReplicaRequest struct {
	Organization string `json:"organization"`
	DatabaseName string `json:"name"`
}

// DatabaseBranchPasswordsService is an interface for communicating with the PlanetScale
// Database Branch Passwords API endpoint.
type DataImportsService interface {
	TestDataImportSource(ctx context.Context, request TestDataImportSourceRequest) (*TestDataImportSourceResponse, error)
}

type dataImportsService struct {
	client *Client
}

// Creates a new password for a branch.
func (d *dataImportsService) TestDataImportSource(ctx context.Context, request TestDataImportSourceRequest) (*TestDataImportSourceResponse, error) {
	path := fmt.Sprintf("/organizations/%s/data-imports/test-connection", request.Organization)
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

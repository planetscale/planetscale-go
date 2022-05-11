package planetscale

import (
	"context"
	"fmt"
	qt "github.com/frankban/quicktest"
	"net/http"
	"net/http/httptest"
	"testing"
)

var knownStates = map[string]DataImportState{
	"prepare_data_copy_pending":        DataImportPreparingDataCopy,
	"prepare_data_copy_error":          DataImportPreparingDataCopyFailed,
	"data_copy_pending":                DataImportCopyingData,
	"data_copy_error":                  DataImportCopyingDataFailed,
	"switch_traffic_workflow_pending":  DataImportSwitchTrafficPending,
	"switch_traffic_workflow_running":  DataImportSwitchTrafficRunning,
	"switch_traffic_workflow_error":    DataImportSwitchTrafficError,
	"reverse_traffic_workflow_running": DataImportReverseTrafficRunning,
	"cleanup_workflow_pending":         DataImportSwitchTrafficCompleted,
	"cleanup_workflow_running":         DataImportDetachExternalDatabaseRunning,
	"cleanup_workflow_error":           DataImportDetachExternalDatabaseError,
	"ready":                            DataImportReady,
}

func TestParseState(t *testing.T) {
	c := qt.New(t)
	for state, importState := range knownStates {
		t.Run(fmt.Sprintf("Can parse state : %s", state), func(t *testing.T) {
			di := DataImport{
				State: state,
			}

			di.ParseState()

			c.Assert(di.ImportState, qt.Equals, importState)
		})
	}
}

func TestCanRunLintExternalDatabase_Success(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{ "can_connect": true, "error": "", "lint_errors": [], "table_statuses": []}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	td := TestDataImportSourceRequest{
		Organization: org,
		Database:     db,
		Source:       DataImportSource{},
	}

	results, err := client.DataImports.TestDataImportSource(ctx, &td)
	c.Assert(err, qt.IsNil)

	c.Assert(true, qt.Equals, results.CanConnect)
}

func TestCanRunLintExternalDatabase_ConnectFailure(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{ "can_connect": false, "error": "external database is down", "lint_errors": [], "table_statuses": []}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	td := TestDataImportSourceRequest{
		Organization: org,
		Database:     db,
		Source:       DataImportSource{},
	}

	results, err := client.DataImports.TestDataImportSource(ctx, &td)
	c.Assert(err, qt.IsNil)

	c.Assert(false, qt.Equals, results.CanConnect)
	c.Assert("external database is down", qt.Equals, results.ConnectError)
}

func TestCanRunLintExternalDatabase_LintFailure(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		out := `{
"can_connect": true, 
"error": "", 
"lint_errors": [{
	"lint_error": "NO_PRIMARY_KEY",
	"table_name": "employees",
	"error_description": "Table 'employees' has no primary key"
}], 
"table_statuses": []
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	td := TestDataImportSourceRequest{
		Organization: org,
		Database:     db,
		Source:       DataImportSource{},
	}

	results, err := client.DataImports.TestDataImportSource(ctx, &td)
	c.Assert(err, qt.IsNil)

	c.Assert(true, qt.Equals, results.CanConnect)
	c.Assert("", qt.Equals, results.ConnectError)
	c.Assert(1, qt.Equals, len(results.Errors))
	c.Assert([]*DataSourceIncompatibilityError{
		&DataSourceIncompatibilityError{
			LintError:        "NO_PRIMARY_KEY",
			Table:            "employees",
			ErrorDescription: "Table 'employees' has no primary key",
		},
	}, qt.DeepEquals, results.Errors)
}

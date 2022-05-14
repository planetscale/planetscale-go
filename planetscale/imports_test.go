package planetscale

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
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

func TestImports_ParseState(t *testing.T) {
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

func TestImports_CanRunLintExternalDatabase_Success(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/data-imports/test-connection")
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

func TestImports_CanRunLintExternalDatabase_ConnectFailure(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/data-imports/test-connection")
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

func TestImports_CanRunLintExternalDatabase_LintFailure(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/data-imports/test-connection")
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

func TestImports_CanStartDataImport_Success(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/data-imports/new")
		w.WriteHeader(200)
		out := `{
"id": "PUBLIC_ID",
"state": "prepare_data_copy_pending", 
"import_check_errors": "",
"data_source": {
	"hostname": "aws.rds.something.com",
	"port": 25060,
	"database": "employees"
}
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"

	startReq := &StartDataImportRequest{
		Organization: org,
		DatabaseName: db,
	}
	di, err := client.DataImports.StartDataImport(ctx, startReq)
	c.Assert(err, qt.IsNil)
	c.Assert(di.ID, qt.Equals, "PUBLIC_ID")
	c.Assert(di.Errors, qt.Equals, "")
	c.Assert(di.ImportState, qt.Equals, DataImportPreparingDataCopy)
}

func TestImports_CanGetDataImportStatus_Success(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db")
		w.WriteHeader(200)
		out := `{
    "id": "planetscale-import-test-db",
    "type": "database",
    "name": "my-db",
    "notes": "This is a test DB created from the planetscale-go API library",
    "created_at": "2021-01-14T10:19:23.000Z",
    "updated_at": "2021-01-14T10:19:23.000Z",
	"data_import": {
	"id": "IMPORT_PUBLIC_ID",
	"state": "switch_traffic_workflow_pending"
}
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	di, err := client.DataImports.GetDataImportStatus(ctx, &GetImportStatusRequest{
		Organization: org,
		Database:     db,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(di.ID, qt.Equals, "IMPORT_PUBLIC_ID")
	c.Assert(di.ImportState, qt.Equals, DataImportSwitchTrafficPending)
}

func TestImports_CanGetDataImportStatus_NoImport(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db")
		w.WriteHeader(200)
		out := `{
    "id": "planetscale-import-test-db",
    "type": "database",
    "name": "my-db",
    "notes": "This is a test DB created from the planetscale-go API library",
    "created_at": "2021-01-14T10:19:23.000Z",
    "updated_at": "2021-01-14T10:19:23.000Z"
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	di, err := client.DataImports.GetDataImportStatus(ctx, &GetImportStatusRequest{
		Organization: org,
		Database:     db,
	})
	c.Assert(err, qt.IsNotNil)
	c.Assert(err, qt.ErrorMatches, "Database my-db is not importing data")
	c.Assert(di, qt.IsNil)
}

func TestImports_CanCancelDataImport(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/data-imports/cancel")
		w.WriteHeader(200)
		out := `{
"id": "PUBLIC_ID",
"state": "prepare_data_copy_pending", 
"import_check_errors": "",
"data_source": {
	"hostname": "aws.rds.something.com",
	"port": "25060",
	"database": "employees"
}
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"
	err = client.DataImports.CancelDataImport(ctx, &CancelDataImportRequest{
		Organization: org,
		Database:     db,
	})
	c.Assert(err, qt.IsNil)
}

func TestImports_CanMakePlanetScalePrimary(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/data-imports/begin-switch-traffic")
		w.WriteHeader(200)
		out := `{
"id": "PUBLIC_ID",
"state": "cleanup_workflow_pending", 
"import_check_errors": "",
"data_source": {
	"hostname": "aws.rds.something.com",
	"port": 25060,
	"database": "employees"
}
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"

	makePrimRequest := &MakePlanetScalePrimaryRequest{
		Organization: org,
		Database:     db,
	}
	di, err := client.DataImports.MakePlanetScalePrimary(ctx, makePrimRequest)
	c.Assert(err, qt.IsNil)
	c.Assert(di.ID, qt.Equals, "PUBLIC_ID")
	c.Assert(di.Errors, qt.Equals, "")
	c.Assert(di.ImportState, qt.Equals, DataImportSwitchTrafficCompleted)
}

func TestImports_CanMakePlanetScaleReplica(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/data-imports/begin-reverse-traffic")
		w.WriteHeader(200)
		out := `{
"id": "PUBLIC_ID",
"state": "switch_traffic_workflow_pending", 
"import_check_errors": "",
"data_source": {
	"hostname": "aws.rds.something.com",
	"port": 25060,
	"database": "employees"
}
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"

	makeReplicaRequest := &MakePlanetScaleReplicaRequest{
		Organization: org,
		Database:     db,
	}
	di, err := client.DataImports.MakePlanetScaleReplica(ctx, makeReplicaRequest)
	c.Assert(err, qt.IsNil)
	c.Assert(di.ID, qt.Equals, "PUBLIC_ID")
	c.Assert(di.Errors, qt.Equals, "")
	c.Assert(di.ImportState, qt.Equals, DataImportSwitchTrafficPending)
}

func TestImports_CanDetachExternalDatabase(t *testing.T) {
	c := qt.New(t)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/data-imports/cleanup")
		w.WriteHeader(200)
		out := `{
"id": "PUBLIC_ID",
"state": "ready", 
"import_check_errors": "",
"data_source": {
	"hostname": "aws.rds.something.com",
	"port": 25060,
	"database": "employees"
}
}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)
	ctx := context.Background()
	org := "my-org"
	db := "my-db"

	detachReq := &DetachExternalDatabaseRequest{
		Organization: org,
		Database:     db,
	}
	di, err := client.DataImports.DetachExternalDatabase(ctx, detachReq)
	c.Assert(err, qt.IsNil)
	c.Assert(di.ID, qt.Equals, "PUBLIC_ID")
	c.Assert(di.Errors, qt.Equals, "")
	c.Assert(di.ImportState, qt.Equals, DataImportReady)

}

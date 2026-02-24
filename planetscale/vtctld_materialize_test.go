package planetscale

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	qt "github.com/frankban/quicktest"
)

func TestMaterialize_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["workflow"], qt.Equals, "my-workflow")
		c.Assert(body["target_keyspace"], qt.Equals, "target")
		c.Assert(body["source_keyspace"], qt.Equals, "source")

		_, hasStopAfterCopy := body["stop_after_copy"]
		_, hasAtomicCopy := body["atomic_copy"]
		_, hasDeferSecondaryKeys := body["defer_secondary_keys"]
		_, hasTabletTypesInPreferenceOrder := body["tablet_types_in_preference_order"]
		c.Assert(hasStopAfterCopy, qt.IsFalse)
		c.Assert(hasAtomicCopy, qt.IsFalse)
		c.Assert(hasDeferSecondaryKeys, qt.IsFalse)
		c.Assert(hasTabletTypesInPreferenceOrder, qt.IsFalse)

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"summary":"created"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Materialize.Create(ctx, &MaterializeCreateRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
		SourceKeyspace: "source",
		TableSettings:  json.RawMessage(`[{"target_table":"t1","source_expression":"select * from t1"}]`),
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"summary":"created"}`)
}

func TestMaterialize_CreateWithExplicitFalseValues(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)

		stopAfterCopy, hasStopAfterCopy := body["stop_after_copy"]
		atomicCopy, hasAtomicCopy := body["atomic_copy"]
		deferSecondaryKeys, hasDeferSecondaryKeys := body["defer_secondary_keys"]
		tabletTypesInPreferenceOrder, hasTabletTypesInPreferenceOrder := body["tablet_types_in_preference_order"]
		c.Assert(hasStopAfterCopy, qt.IsTrue)
		c.Assert(hasAtomicCopy, qt.IsTrue)
		c.Assert(hasDeferSecondaryKeys, qt.IsTrue)
		c.Assert(hasTabletTypesInPreferenceOrder, qt.IsTrue)
		c.Assert(stopAfterCopy, qt.Equals, false)
		c.Assert(atomicCopy, qt.Equals, false)
		c.Assert(deferSecondaryKeys, qt.Equals, false)
		c.Assert(tabletTypesInPreferenceOrder, qt.Equals, false)

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"summary":"created"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	falseValue := false
	ctx := context.Background()
	data, err := client.Materialize.Create(ctx, &MaterializeCreateRequest{
		Organization:                 "my-org",
		Database:                     "my-db",
		Branch:                       "my-branch",
		Workflow:                     "my-workflow",
		TargetKeyspace:               "target",
		SourceKeyspace:               "source",
		TableSettings:                json.RawMessage(`[{"target_table":"t1","source_expression":"select * from t1"}]`),
		StopAfterCopy:                &falseValue,
		AtomicCopy:                   &falseValue,
		DeferSecondaryKeys:           &falseValue,
		TabletTypesInPreferenceOrder: &falseValue,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"summary":"created"}`)
}

func TestMaterialize_Show(t *testing.T) {
	testCases := []struct {
		name               string
		includeLogs        *bool
		expectIncludeParam bool
		expectedInclude    string
	}{
		{name: "include_logs true", includeLogs: boolPtr(true), expectIncludeParam: true, expectedInclude: "true"},
		{name: "include_logs false", includeLogs: boolPtr(false), expectIncludeParam: true, expectedInclude: "false"},
		{name: "include_logs omitted", includeLogs: nil, expectIncludeParam: false},
	}

	for _, tc := range testCases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			c := qt.New(t)

			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				c.Assert(r.Method, qt.Equals, http.MethodGet)
				c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows/my-workflow")
				c.Assert(r.URL.Query().Get("target_keyspace"), qt.Equals, "target")

				_, hasIncludeLogs := r.URL.Query()["include_logs"]
				if tc.expectIncludeParam {
					c.Assert(hasIncludeLogs, qt.IsTrue)
					c.Assert(r.URL.Query().Get("include_logs"), qt.Equals, tc.expectedInclude)
				} else {
					c.Assert(hasIncludeLogs, qt.IsFalse)
				}

				w.WriteHeader(200)
				_, err := w.Write([]byte(`{"data":{"result":"ok"}}`))
				c.Assert(err, qt.IsNil)
			}))
			defer ts.Close()

			client, err := NewClient(WithBaseURL(ts.URL))
			c.Assert(err, qt.IsNil)

			ctx := context.Background()
			data, err := client.Materialize.Show(ctx, &MaterializeShowRequest{
				Organization:   "my-org",
				Database:       "my-db",
				Branch:         "my-branch",
				Workflow:       "my-workflow",
				TargetKeyspace: "target",
				IncludeLogs:    tc.includeLogs,
			})
			c.Assert(err, qt.IsNil)
			c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
		})
	}
}

func TestMaterialize_Start(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows/my-workflow/start")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Materialize.Start(ctx, &MaterializeStartRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMaterialize_Stop(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows/my-workflow/stop")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Materialize.Stop(ctx, &MaterializeStopRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMaterialize_Cancel(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows/my-workflow/cancel")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)
		c.Assert(body["target_keyspace"], qt.Equals, "target")

		_, hasKeepData := body["keep_data"]
		_, hasKeepRoutingRules := body["keep_routing_rules"]
		c.Assert(hasKeepData, qt.IsFalse)
		c.Assert(hasKeepRoutingRules, qt.IsFalse)

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()
	data, err := client.Materialize.Cancel(ctx, &MaterializeCancelRequest{
		Organization:   "my-org",
		Database:       "my-db",
		Branch:         "my-branch",
		Workflow:       "my-workflow",
		TargetKeyspace: "target",
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

func TestMaterialize_CancelWithExplicitFalseValues(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.Path, qt.Equals, "/v1/organizations/my-org/databases/my-db/branches/my-branch/materialize/workflows/my-workflow/cancel")

		var body map[string]interface{}
		err := json.NewDecoder(r.Body).Decode(&body)
		c.Assert(err, qt.IsNil)

		keepData, hasKeepData := body["keep_data"]
		keepRoutingRules, hasKeepRoutingRules := body["keep_routing_rules"]
		c.Assert(hasKeepData, qt.IsTrue)
		c.Assert(hasKeepRoutingRules, qt.IsTrue)
		c.Assert(keepData, qt.Equals, false)
		c.Assert(keepRoutingRules, qt.Equals, false)

		w.WriteHeader(200)
		_, err = w.Write([]byte(`{"data":{"result":"ok"}}`))
		c.Assert(err, qt.IsNil)
	}))
	defer ts.Close()

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	falseValue := false
	ctx := context.Background()
	data, err := client.Materialize.Cancel(ctx, &MaterializeCancelRequest{
		Organization:     "my-org",
		Database:         "my-db",
		Branch:           "my-branch",
		Workflow:         "my-workflow",
		TargetKeyspace:   "target",
		KeepData:         &falseValue,
		KeepRoutingRules: &falseValue,
	})
	c.Assert(err, qt.IsNil)
	c.Assert(string(data), qt.Equals, `{"result":"ok"}`)
}

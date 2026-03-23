package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestTrafficRules_Create(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodPost)
		c.Assert(r.URL.String(), qt.Equals, "/v1/organizations/my-org/databases/planetscale-go-test-db/branches/planetscale-go-test-db-branch/traffic/budgets/qok87ki4xlau/rules")

		w.WriteHeader(200)
		out := `{
			"id":"j1d8ecc2su4e",
			"type":"TrafficRule",
			"kind":"match",
			"tags":[
				{"type":"TrafficRuleTag","key_id":"Squery","key":"query","value":"a_query","source":"sql"}
			],
			"fingerprint":null,
			"keyspace":null,
			"created_at":"2026-03-20T16:05:58.461Z",
			"updated_at":"2026-03-20T16:05:58.461Z",
			"actor":{"id":"v1bxjxtt9c13","type":"User","display_name":"Alice"}
		}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	rule, err := client.TrafficRules.Create(ctx, &CreateTrafficRuleRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Branch:       testBranch,
		BudgetID:     "qok87ki4xlau",
		Kind:         "match",
		Tags: []TrafficRuleTag{
			{Key: "query", Value: "a_query", Source: "sql"},
		},
	})

	want := &TrafficRule{
		ID:   "j1d8ecc2su4e",
		Type: "TrafficRule",
		Kind: "match",
		Tags: []TrafficRuleTag{
			{Type: "TrafficRuleTag", KeyID: "Squery", Key: "query", Value: "a_query", Source: "sql"},
		},
		Actor:     Actor{ID: "v1bxjxtt9c13", Type: "User", Name: "Alice"},
		CreatedAt: time.Date(2026, time.March, 20, 16, 5, 58, 461000000, time.UTC),
		UpdatedAt: time.Date(2026, time.March, 20, 16, 5, 58, 461000000, time.UTC),
	}

	c.Assert(err, qt.IsNil)
	c.Assert(rule, qt.DeepEquals, want)
}

func TestTrafficRules_Delete(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c.Assert(r.Method, qt.Equals, http.MethodDelete)
		c.Assert(r.URL.String(), qt.Equals, "/v1/organizations/my-org/databases/planetscale-go-test-db/branches/planetscale-go-test-db-branch/traffic/budgets/qok87ki4xlau/rules/j1d8ecc2su4e")

		w.WriteHeader(http.StatusNoContent)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	err = client.TrafficRules.Delete(ctx, &DeleteTrafficRuleRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Branch:       testBranch,
		BudgetID:     "qok87ki4xlau",
		RuleID:       "j1d8ecc2su4e",
	})

	c.Assert(err, qt.IsNil)
}

package planetscale

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

func TestBranchInfrastructure_Get(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.String(), qt.Equals, "/v1/organizations/my-org/databases/planetscale-go-test-db/branches/planetscale-go-test-db-branch/infrastructure")

		out := `{
			"type": "PS-10",
			"ready": true,
			"pods": [
				{
					"name": "vttablet-abc123",
					"status": "Running",
					"component": "vttablet",
					"ready": "1/1",
					"restart_count": 0,
					"created_at": "2021-01-14T10:19:23.000Z",
					"cell": "us-east-1",
					"size": "PS-10",
					"keyspace": "main",
					"shard": "-",
					"tablet_type": "primary"
				},
				{
					"name": "vtgate-def456",
					"status": "Running",
					"component": "vtgate",
					"ready": "1/1",
					"restart_count": 2,
					"created_at": "2021-01-14T10:19:23.000Z",
					"cell": "us-east-1",
					"size": "PS-10",
					"keyspace": null,
					"shard": null,
					"tablet_type": null
				}
			]
		}`
		_, err := w.Write([]byte(out))
		c.Assert(err, qt.IsNil)
	}))

	client, err := NewClient(WithBaseURL(ts.URL))
	c.Assert(err, qt.IsNil)

	ctx := context.Background()

	infra, err := client.BranchInfrastructure.Get(ctx, &GetBranchInfrastructureRequest{
		Organization: testOrg,
		Database:     testDatabase,
		Branch:       testBranch,
	})

	createdAt := time.Date(2021, 1, 14, 10, 19, 23, 0, time.UTC)
	keyspace := "main"
	shard := "-"
	tabletType := "primary"

	c.Assert(err, qt.IsNil)
	c.Assert(infra.Type, qt.Equals, "PS-10")
	c.Assert(infra.Ready, qt.IsTrue)
	c.Assert(len(infra.Pods), qt.Equals, 2)

	c.Assert(infra.Pods[0].Name, qt.Equals, "vttablet-abc123")
	c.Assert(infra.Pods[0].Status, qt.Equals, "Running")
	c.Assert(infra.Pods[0].Component, qt.Equals, "vttablet")
	c.Assert(infra.Pods[0].Ready, qt.Equals, "1/1")
	c.Assert(infra.Pods[0].RestartCount, qt.Equals, 0)
	c.Assert(infra.Pods[0].CreatedAt, qt.DeepEquals, &createdAt)
	c.Assert(infra.Pods[0].Cell, qt.Equals, "us-east-1")
	c.Assert(infra.Pods[0].Size, qt.Equals, "PS-10")
	c.Assert(infra.Pods[0].Keyspace, qt.DeepEquals, &keyspace)
	c.Assert(infra.Pods[0].Shard, qt.DeepEquals, &shard)
	c.Assert(infra.Pods[0].TabletType, qt.DeepEquals, &tabletType)

	c.Assert(infra.Pods[1].Name, qt.Equals, "vtgate-def456")
	c.Assert(infra.Pods[1].Component, qt.Equals, "vtgate")
	c.Assert(infra.Pods[1].RestartCount, qt.Equals, 2)
	c.Assert(infra.Pods[1].Keyspace, qt.IsNil)
	c.Assert(infra.Pods[1].Shard, qt.IsNil)
	c.Assert(infra.Pods[1].TabletType, qt.IsNil)
}

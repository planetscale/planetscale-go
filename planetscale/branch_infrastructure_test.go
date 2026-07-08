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
	c.Assert(infra.Postgres, qt.IsNil)
	c.Assert(infra.Vitess, qt.IsNotNil)
	c.Assert(infra.Vitess.Ready, qt.IsTrue)
	c.Assert(len(infra.Vitess.Pods), qt.Equals, 2)

	pods := infra.Vitess.Pods
	c.Assert(pods[0].Name, qt.Equals, "vttablet-abc123")
	c.Assert(pods[0].Status, qt.Equals, "Running")
	c.Assert(pods[0].Component, qt.Equals, "vttablet")
	c.Assert(pods[0].Ready, qt.Equals, "1/1")
	c.Assert(pods[0].RestartCount, qt.Equals, 0)
	c.Assert(pods[0].CreatedAt, qt.DeepEquals, &createdAt)
	c.Assert(pods[0].Cell, qt.Equals, "us-east-1")
	c.Assert(pods[0].Size, qt.Equals, "PS-10")
	c.Assert(pods[0].Keyspace, qt.DeepEquals, &keyspace)
	c.Assert(pods[0].Shard, qt.DeepEquals, &shard)
	c.Assert(pods[0].TabletType, qt.DeepEquals, &tabletType)

	c.Assert(pods[1].Name, qt.Equals, "vtgate-def456")
	c.Assert(pods[1].Component, qt.Equals, "vtgate")
	c.Assert(pods[1].RestartCount, qt.Equals, 2)
	c.Assert(pods[1].Keyspace, qt.IsNil)
	c.Assert(pods[1].Shard, qt.IsNil)
	c.Assert(pods[1].TabletType, qt.IsNil)
}

func TestBranchInfrastructure_GetPostgres(t *testing.T) {
	c := qt.New(t)

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(200)
		c.Assert(r.Method, qt.Equals, http.MethodGet)
		c.Assert(r.URL.String(), qt.Equals, "/v1/organizations/my-org/databases/planetscale-go-test-db/branches/planetscale-go-test-db-branch/infrastructure")

		out := `{
			"type": "PostgresInfrastructure",
			"state": "ready",
			"primary_name": "hzi-abc123-aws-useast2a-1-1735266582-9c83d493",
			"primary_promoted_at": "2025-12-09T13:15:00.000Z",
			"volume_modifications_blocked_until": null,
			"nodes": [
				{
					"type": "PostgresNode",
					"cluster_display_name": "PS-40",
					"cluster_name": "PS_40",
					"availability_zone": "Availability zone A",
					"normalized_name": "aws-useast2a-1",
					"name": "hzi-abc123-aws-useast2a-1-1735266582-9c83d493",
					"peers_count": 0,
					"role": "primary",
					"volume_usage_bytes": 325369856,
					"volume_capacity_bytes": 10737418240,
					"volume_shrink_threshold_percent": 12.5,
					"region": {
						"slug": "us-east-2",
						"provider": "AWS",
						"display_name": "AWS us-east-2",
						"enabled": true
					},
					"disk_replacement": {
						"type": "PostgresNodeDiskReplacement",
						"reason": "ShrinkSuggested",
						"bytes": 21474836480,
						"scheduled_at": "2025-12-09T14:00:00.000Z"
					}
				},
				{
					"type": "PostgresNode",
					"cluster_display_name": "PS-40",
					"cluster_name": "PS_40",
					"availability_zone": "Availability zone C",
					"normalized_name": "aws-useast2c-1",
					"name": "hzi-abc123-aws-useast2c-1-3012702306-89b0e759",
					"peers_count": 0,
					"role": "replica",
					"volume_usage_bytes": 341385216,
					"volume_capacity_bytes": 10737418240,
					"volume_shrink_threshold_percent": null,
					"region": {
						"slug": "us-east-2",
						"provider": "AWS",
						"display_name": "AWS us-east-2",
						"enabled": true
					},
					"disk_replacement": null
				}
			],
			"bouncers": [
				{
					"type": "PostgresBouncerNode",
					"id": "bouncer123",
					"name": "primary-bouncer",
					"target": "primary",
					"replicas_per_cell": 1,
					"region": {
						"slug": "us-east-2",
						"provider": "AWS",
						"display_name": "AWS us-east-2",
						"enabled": true
					},
					"sku": {
						"name": "HZB_C1_AMD64_PICO",
						"display_name": "Pico",
						"cpu": "0.25",
						"ram": 268435456
					}
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

	c.Assert(err, qt.IsNil)
	c.Assert(infra.Type, qt.Equals, "PostgresInfrastructure")
	c.Assert(infra.Vitess, qt.IsNil)
	c.Assert(infra.Postgres, qt.IsNotNil)

	pg := infra.Postgres
	c.Assert(pg.State, qt.Equals, "ready")
	c.Assert(pg.PrimaryName, qt.Equals, "hzi-abc123-aws-useast2a-1-1735266582-9c83d493")
	c.Assert(pg.PrimaryPromotedAt, qt.DeepEquals, ptrTime(time.Date(2025, 12, 9, 13, 15, 0, 0, time.UTC)))
	c.Assert(pg.VolumeModificationsBlockedUntil, qt.IsNil)
	c.Assert(len(pg.Nodes), qt.Equals, 2)
	c.Assert(len(pg.Bouncers), qt.Equals, 1)

	primary := pg.Nodes[0]
	c.Assert(primary.Name, qt.Equals, "hzi-abc123-aws-useast2a-1-1735266582-9c83d493")
	c.Assert(primary.NormalizedName, qt.Equals, "aws-useast2a-1")
	c.Assert(primary.Role, qt.Equals, "primary")
	c.Assert(primary.AvailabilityZone, qt.Equals, "Availability zone A")
	c.Assert(primary.ClusterName, qt.Equals, "PS_40")
	c.Assert(primary.ClusterDisplayName, qt.Equals, "PS-40")
	c.Assert(*primary.VolumeUsageBytes, qt.Equals, int64(325369856))
	c.Assert(*primary.VolumeCapacityBytes, qt.Equals, int64(10737418240))
	c.Assert(*primary.VolumeShrinkThresholdPercent, qt.Equals, 12.5)
	c.Assert(primary.Region.Slug, qt.Equals, "us-east-2")
	c.Assert(primary.DiskReplacement.Reason, qt.Equals, "ShrinkSuggested")
	c.Assert(primary.DiskReplacement.Bytes, qt.Equals, int64(21474836480))
	c.Assert(primary.DiskReplacement.ScheduledAt, qt.DeepEquals, ptrTime(time.Date(2025, 12, 9, 14, 0, 0, 0, time.UTC)))

	replica := pg.Nodes[1]
	c.Assert(replica.Role, qt.Equals, "replica")
	c.Assert(replica.VolumeShrinkThresholdPercent, qt.IsNil)
	c.Assert(replica.DiskReplacement, qt.IsNil)

	bouncer := pg.Bouncers[0]
	c.Assert(bouncer.ID, qt.Equals, "bouncer123")
	c.Assert(bouncer.Name, qt.Equals, "primary-bouncer")
	c.Assert(bouncer.Target, qt.Equals, "primary")
	c.Assert(bouncer.ReplicasPerCell, qt.Equals, 1)
	c.Assert(bouncer.Region.Slug, qt.Equals, "us-east-2")
	c.Assert(bouncer.SKU.Name, qt.Equals, "HZB_C1_AMD64_PICO")
	c.Assert(bouncer.SKU.DisplayName, qt.Equals, "Pico")
	c.Assert(bouncer.SKU.CPU, qt.Equals, "0.25")
	c.Assert(bouncer.SKU.RAM, qt.Equals, int64(268435456))
}

func ptrTime(t time.Time) *time.Time {
	return &t
}

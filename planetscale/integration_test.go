//go:build integration
// +build integration

package planetscale

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"
)

// This integration test creates, lists and then deletes a PlanetScale
// Database. Use with caution!. Usage:
//
//   PLANETSCALE_TOKEN=$(cat ~/.config/planetscale/access-token) PLANETSCALE_ORG="damp-dew-9934" go test -tags integration
//

func TestIntegration_Databases_List(t *testing.T) {
	token := os.Getenv("PLANETSCALE_TOKEN")
	if token == "" {
		t.Fatalf("PLANETSCALE_TOKEN is not set")
	}

	org := os.Getenv("PLANETSCALE_ORG")
	if org == "" {
		t.Fatalf("PLANETSCALE_ORG is not set")
	}

	ctx := context.Background()

	client, err := NewClient(
		WithAccessToken(token),
	)
	if err != nil {
		t.Fatal(err)
	}

	dbName := "planetscale-go-test-db"

	_, err = client.Databases.Create(ctx, &CreateDatabaseRequest{
		Organization: org,
		Name:         dbName,
	})
	if err != nil {
		t.Fatalf("create database failed: %s", err)
	}

	// poor mans polling, remove once we have an API to poll the status of the DB
	time.Sleep(time.Second * 2)

	dbs, err := client.Databases.List(ctx, &ListDatabasesRequest{
		Organization: org,
	})
	if err != nil {
		t.Fatalf("list database failed: %s", err)
	}

	fmt.Printf("Found %d databases\n", len(dbs))
	for _, db := range dbs {
		fmt.Printf("db struct = %+v\n", db)
		fmt.Println("----------")
		fmt.Printf("Name: %q\n", db.Name)
		fmt.Printf("Notes: %q\n", db.Notes)
	}

	err = client.Databases.Delete(ctx, &DeleteDatabaseRequest{
		Organization: org,
		Database:     dbName,
	})
	if err != nil {
		t.Fatalf("delete database failed: %s", err)
	}
}

func TestIntegration_AuditLogs_List(t *testing.T) {
	token := os.Getenv("PLANETSCALE_TOKEN")
	if token == "" {
		t.Fatalf("PLANETSCALE_TOKEN is not set")
	}

	org := os.Getenv("PLANETSCALE_ORG")
	if org == "" {
		t.Fatalf("PLANETSCALE_ORG is not set")
	}

	ctx := context.Background()

	client, err := NewClient(
		WithAccessToken(token),
	)
	if err != nil {
		t.Fatal(err)
	}

	auditLogs, err := client.AuditLogs.List(ctx, &ListAuditLogsRequest{
		Organization: org,
		Events: []AuditLogEvent{
			AuditLogEventBranchDeleted,
			AuditLogEventOrganizationJoined,
		},
	})
	if err != nil {
		t.Fatalf("get audit logs failed: %s", err)
	}

	for _, l := range auditLogs {
		fmt.Printf("l. = %+v\n", l.AuditAction)
	}
	fmt.Printf("len(auditLogs) = %+v\n", len(auditLogs))
}

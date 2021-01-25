// +build integration

package planetscale

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"fmt"
	"os"
	"testing"
	"time"

	qt "github.com/frankban/quicktest"
)

// This integration test creates, lists and then deletes a PlanetScale
// Database. Use with caution!. Usage:
//
//   PLANETSCALE_TOKEN=$(cat ~/.config/planetscale/access-token) PLANETSCALE_ORG="damp-dew-9934" go test -tags integration
//

func TestIntegration_Certificate_Create(t *testing.T) {
	c := qt.New(t)
	token := os.Getenv("PLANETSCALE_TOKEN")
	c.Assert(token, qt.Not(qt.Equals), "", qt.Commentf("PLANETSCALE_TOKEN is not set"))

	org := os.Getenv("PLANETSCALE_ORG")
	c.Assert(org, qt.Not(qt.Equals), "", qt.Commentf("PLANETSCALE_ORG is not set"))

	ctx := context.Background()

	client, err := NewClient(
		WithAccessToken(token),
	)
	c.Assert(err, qt.IsNil)

	pkey, err := rsa.GenerateKey(rand.Reader, 2048)
	c.Assert(err, qt.IsNil)

	cert, err := client.Certificates.Create(ctx, &CreateCertificateRequest{
		Organization: org,
		DatabaseName: "fatihs-db",
		Branch:       "development",
		PrivateKey:   pkey,
	})
	c.Assert(err, qt.IsNil)

	fmt.Printf("cert = %+v\n", cert)
}

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
		Database: &Database{
			Name:  dbName,
			Notes: "This is a test DB created from the planetscale-go API library",
		},
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

	_, err = client.Databases.Delete(ctx, &DeleteDatabaseRequest{
		Organization: org,
		Database:     dbName,
	})
	if err != nil {
		t.Fatalf("delete database failed: %s", err)
	}
}

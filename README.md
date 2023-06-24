# planetscale-go [![Go Reference](https://pkg.go.dev/badge/github.com/planetscale/planetscale-go/planetscale.svg)](https://pkg.go.dev/github.com/planetscale/planetscale-go/planetscale) [![Build status](https://badge.buildkite.com/82dafa9518fe94b3fed75db71bcfc3836faeec49816e400f2e.svg?branch=main)](https://buildkite.com/planetscale/planetscale-go)

Go package to access the PlanetScale API.

## Install

```
go get github.com/planetscale/planetscale-go/planetscale
```

## Usage

Here is an example application using the PlanetScale Go client. You can create
and manage your service tokens via our [pscale
CLI](https://github.com/planetscale/cli) with the `pscale service-token`
subcommand.

```go
package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/planetscale/planetscale-go/planetscale"
)

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a new PlanetScale API client with the given service token.
	client, err := planetscale.NewClient(
		planetscale.WithServiceToken("token-id", os.Getenv("PLANETSCALE_TOKEN")),
	)
	if err != nil {
		log.Fatalf("failed to create client: %v", err)
	}

	// Create a new database.
	_, err = client.Databases.Create(ctx, &planetscale.CreateDatabaseRequest{
		Organization: "my-org",
		Name:         "my-awesome-database",
		Notes:        "This is a test DB created via the planetscale-go API library",
	})
	if err != nil {
		log.Fatalf("failed to create database: %v", err)
	}

	// List all databases for the given organization.
	databases, err := client.Databases.List(ctx, &planetscale.ListDatabasesRequest{
		Organization: "my-org",
	})
	if err != nil {
		log.Fatalf("failed to list databases: %v", err)
	}

	log.Printf("found %d databases:", len(databases))
	for _, db := range databases {
		log.Printf("  - %q: %s", db.Name, db.Notes)
	}

	// Delete a database.
	_, err = client.Databases.Delete(ctx, &planetscale.DeleteDatabaseRequest{
		Organization: "my-org",
		Database:     "my-awesome-database",
	})
	if err != nil {
		log.Fatalf("failed to delete database: %v", err)
	}
}
```

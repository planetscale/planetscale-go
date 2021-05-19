# planetscale-go

Go package to access the PlanetScale API.


## Install

```bash
go get github.com/planetscale/planetscale-go/planetscale
```

## Usage

Here is an example usage of the PlanetScale Go client. Please make sure to
handle errors in your production application.


```go
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/planetscale/planetscale-go/planetscale"
)

func main() {
	token := os.Getenv("PLANETSCALE_TOKEN")

	ctx := context.Background()

	// create a new PlanetScale API client with the given access token
	client, _ := planetscale.NewClient(
		planetscale.WithAccessToken(token),
	)

	// create a new database
	_, err := client.Databases.Create(ctx, &planetscale.CreateDatabaseRequest{
		Organization: "my-org",
		Name:         "my-awesome-database",
		Notes:        "This is a test DB created via the planetscale-go API library",
	})

	// list all databases for the given organization
	databases, _ := client.Databases.List(ctx, &planetscale.ListDatabasesRequest{
		Organization: "my-org",
	})
	fmt.Printf("Found %d databases\n", len(databases))
	for _, db := range databases {
		fmt.Printf("Name: %q\n", db.Name)
		fmt.Printf("Notes: %q\n", db.Notes)
	}

	// delete a database
	_ = client.Databases.Delete(ctx, &planetscale.DeleteDatabaseRequest{
		Organization: "my-org",
		Database:     "my-awesome-database",
	})
}
```


## Use a custom HTTP Client

You can use a custom HTTP Client with the `planetscale.WithHTTPClient()` option
function:

```go
httpClient := &http.Client{
	Timeout: 15 * time.Second,
}

// create a new PlanetScale API client with the given access token and
// custom HTTP Client
client, _ := planetscale.NewClient(
	planetscale.WithHTTPClient(httpClient),
	planetscale.WithAccessToken(token),
)
```

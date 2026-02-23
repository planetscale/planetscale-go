package planetscale

import "encoding/json"

// vtctldDataResponse is the shared response envelope for all vtctld API responses.
// The Data field contains the raw JSON response from the vtctld command.
//
//lint:ignore U1000 used by service files in dependent PRs
type vtctldDataResponse struct {
	Data json.RawMessage `json:"data"`
}

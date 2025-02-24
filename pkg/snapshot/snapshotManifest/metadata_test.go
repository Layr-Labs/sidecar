package snapshotManifest

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"testing"
	"time"
)

func Test_CreatedAtTimeParsing(t *testing.T) {
	inputJson := `
		{
			"createdAt": "2025-08-10T15:00:00Z"
		}
	`
	var snapshot *Snapshot
	err := json.Unmarshal([]byte(inputJson), &snapshot)
	assert.Nil(t, err)
	assert.Equal(t, snapshot.CreatedAt.Format(time.RFC3339), "2025-08-10T15:00:00Z")
}

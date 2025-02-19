package snapshotManifest

import (
	"encoding/json"
	"fmt"
	"github.com/rogpeppe/go-internal/semver"
	"time"
)

type CreatedAt struct {
	time.Time
}

func (ca *CreatedAt) UnmarshalJSON(data []byte) error {
	timeString := string(data)
	if timeString == "" {
		return nil
	}
	// check to make sure the timestamp is quoted properly
	if timeString[0] != '"' || timeString[len(timeString)-1] != '"' {
		return fmt.Errorf("Invalid value provided for createdAt '%s'", timeString)
	}
	// remove the quotes
	timeString = timeString[1 : len(timeString)-1]

	t, err := time.Parse(time.RFC3339, timeString)
	if err != nil {
		return err
	}
	ca.Time = t
	return nil
}

type Snapshot struct {
	SidecarVersion string    `json:"sidecarVersion"`
	Chain          string    `json:"chain"`
	CreatedAt      CreatedAt `json:"createdAt"`
	Url            string    `json:"url"`
	Schema         string    `json:"schema"`
	Kind           string    `json:"kind"`
}

type Metadata struct {
	Version string `json:"version"`
}

type SnapshotManifest struct {
	Metadata  Metadata    `json:"metadata"`
	Snapshots []*Snapshot `json:"snapshots"`
}

func (sm *SnapshotManifest) FindSnapshot(chain string, sidecarVersion string, schemaName string) *Snapshot {
	if len(sm.Snapshots) == 0 {
		return nil
	}

	for _, snapshot := range sm.Snapshots {
		if snapshot.Chain != chain {
			continue
		}
		if snapshot.Schema != schemaName {
			continue
		}
		snapshotVersion := snapshot.SidecarVersion

		// Find the first version in the list where the snapshot is equal to or less than the sidecar version.
		if cmp := semver.Compare(snapshotVersion, sidecarVersion); cmp <= 0 {
			return snapshot
		}
	}
	return nil
}

func NewSnapshotManifestFromJson(data []byte) (*SnapshotManifest, error) {
	var manifest *SnapshotManifest

	if err := json.Unmarshal(data, manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

// Package snapshotManifest provides functionality for working with snapshot manifests.
// A snapshot manifest is a JSON file containing information about available snapshots
// for database restoration.
package snapshotManifest

import (
	"encoding/json"
	"fmt"
	"golang.org/x/mod/semver"
	"time"
)

// CreatedAt is a wrapper around time.Time that provides custom JSON unmarshaling
// for timestamp formats.
type CreatedAt struct {
	time.Time
}

// UnmarshalJSON implements the json.Unmarshaler interface for CreatedAt.
// It parses JSON strings containing RFC3339 timestamps and returns an error
// for invalid formats.
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

// Snapshot represents a single database snapshot in the manifest.
// It contains information about the snapshot's version, chain, creation time,
// URL, schema, kind, and signature.
type Snapshot struct {
	// SidecarVersion is the version of sidecar that created the snapshot
	SidecarVersion string `json:"sidecarVersion"`
	// Chain identifies the blockchain network for the snapshot
	Chain string `json:"chain"`
	// CreatedAt is the timestamp when the snapshot was created
	CreatedAt CreatedAt `json:"createdAt"`
	// Url is the location where the snapshot file can be downloaded
	Url string `json:"url"`
	// Schema is the database schema included in the snapshot
	Schema string `json:"schema"`
	// Kind specifies the type of snapshot (Slim, Full, Archive)
	Kind string `json:"kind"`
	// Signature is the cryptographic signature for the snapshot file
	Signature string `json:"signature"`
}

// Metadata contains version information for the snapshot manifest.
type Metadata struct {
	// Version is the version of the manifest format
	Version string `json:"version"`
}

// SnapshotManifest represents a collection of available snapshots along with metadata.
// It provides methods for finding compatible snapshots based on specified criteria.
type SnapshotManifest struct {
	// Metadata contains version information for the manifest
	Metadata Metadata `json:"metadata"`
	// Snapshots is a list of available snapshots
	Snapshots []*Snapshot `json:"snapshots"`
}

// FindSnapshot searches for a compatible snapshot based on the specified criteria.
// It looks for a snapshot matching the chain, schema, and kind, with a version
// that is equal to or less than the specified sidecar version.
// Returns the first matching snapshot, or nil if no match is found.
func (sm *SnapshotManifest) FindSnapshot(chain string, sidecarVersion string, schemaName string, kind string) *Snapshot {
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
		if kind != "" && snapshot.Kind != kind {
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

// NewSnapshotManifestFromJson creates a new SnapshotManifest from JSON data.
// It unmarshals the JSON data into a SnapshotManifest struct and returns it.
// Returns an error if the JSON data cannot be unmarshaled.
func NewSnapshotManifestFromJson(data []byte) (*SnapshotManifest, error) {
	var manifest *SnapshotManifest

	if err := json.Unmarshal(data, &manifest); err != nil {
		return nil, err
	}
	return manifest, nil
}

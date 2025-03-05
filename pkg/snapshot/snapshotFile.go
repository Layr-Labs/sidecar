package snapshot

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"github.com/ProtonMail/go-crypto/openpgp"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// SnapshotFile represents a database snapshot file and its associated metadata.
// It provides methods for managing snapshot files, including hash generation,
// validation, and metadata handling.
type SnapshotFile struct {
	// Dir is the directory containing the snapshot file
	Dir string
	// SnapshotFileName is the name of the snapshot file
	SnapshotFileName string
	// CreatedTimestamp is the time when the snapshot was created
	CreatedTimestamp time.Time
	// Chain identifies the blockchain network for the snapshot
	Chain string
	// Version is the sidecar version used to create the snapshot
	Version string
	// SchemaName is the database schema included in the snapshot
	SchemaName string
	// Kind specifies the type of snapshot (Slim, Full, Archive)
	Kind string
}

// SnapshotMetadata contains metadata about a snapshot for serialization to JSON.
type SnapshotMetadata struct {
	// Version is the sidecar version used to create the snapshot
	Version string `json:"version"`
	// Chain identifies the blockchain network for the snapshot
	Chain string `json:"chain"`
	// Schema is the database schema included in the snapshot
	Schema string `json:"schema"`
	// Kind specifies the type of snapshot (Slim, Full, Archive)
	Kind string `json:"kind"`
	// Timestamp is the creation time of the snapshot in RFC3339 format
	Timestamp string `json:"timestamp"`
	// FileName is the name of the snapshot file
	FileName string `json:"fileName"`
}

// HashExt returns the file extension for hash files.
func (sf *SnapshotFile) HashExt() string {
	return "sha256"
}

// SignatureExt returns the file extension for signature files.
func (sf *SnapshotFile) SignatureExt() string {
	return "asc"
}

// HashFileName returns the filename for the snapshot's hash file.
func (sf *SnapshotFile) HashFileName() string {
	return fmt.Sprintf("%s.%s", sf.SnapshotFileName, sf.HashExt())
}

// SignatureFileName returns the filename for the snapshot's signature file.
func (sf *SnapshotFile) SignatureFileName() string {
	return fmt.Sprintf("%s.%s", sf.SnapshotFileName, sf.SignatureExt())
}

// FullPath returns the absolute path to the snapshot file.
func (sf *SnapshotFile) FullPath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.SnapshotFileName)
}

// HashFilePath returns the absolute path to the snapshot's hash file.
func (sf *SnapshotFile) HashFilePath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.HashFileName())
}

// SignatureFilePath returns the absolute path to the snapshot's signature file.
func (sf *SnapshotFile) SignatureFilePath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.SignatureFileName())
}

// ValidateHash verifies the integrity of the snapshot file by comparing
// its computed hash with the hash stored in the hash file.
// It returns an error if the hashes don't match or if any operation fails.
func (sf *SnapshotFile) ValidateHash() error {
	hashFile, err := os.ReadFile(sf.HashFilePath())
	if err != nil {
		return fmt.Errorf("error reading hash file: %w", err)
	}
	// hash file layout:
	// 0x<hash> <filename>

	hashString := strings.Fields(string(hashFile))[0]

	sum, err := sf.GenerateSnapshotHash()
	if err != nil {
		return fmt.Errorf("error generating snapshot hash: %w", err)
	}

	if sum != hashString {
		return fmt.Errorf("hashes do not match: %s != %s", sum, hashString)
	}
	return nil
}

// GenerateSnapshotHash computes the SHA-256 hash of the snapshot file.
// It processes the file in 1MB chunks to handle large files efficiently.
// Returns the hexadecimal representation of the hash and any error encountered.
func (sf *SnapshotFile) GenerateSnapshotHash() (string, error) {
	dumpFile, err := os.Open(sf.FullPath())
	if err != nil {
		return "", fmt.Errorf("error opening snapshot file: %w", err)
	}

	hash := sha256.New()
	buf := make([]byte, 1024*1024)

	// snapshots are multiple gigabytes, so this breaks it into 1MB chunks
	for {
		n, err := dumpFile.Read(buf)
		if n > 0 {
			hash.Write(buf[:n])
		}
		if err == io.EOF {
			break
		}
		if err != nil {
			return "", fmt.Errorf("error reading snapshot file: %w", err)
		}
	}

	sum := strings.TrimPrefix(hexutil.Encode(hash.Sum(nil)), "0x")
	return sum, nil
}

// GenerateAndSaveSnapshotHash computes the SHA-256 hash of the snapshot file
// and saves it to a hash file. The hash file contains the hash and the snapshot filename.
// Returns an error if any operation fails.
func (sf *SnapshotFile) GenerateAndSaveSnapshotHash() error {
	sum, err := sf.GenerateSnapshotHash()
	if err != nil {
		return fmt.Errorf("error generating snapshot hash: %w", err)
	}

	hashFile, err := os.OpenFile(sf.HashFilePath(), os.O_CREATE|os.O_WRONLY, 0775)
	if err != nil {
		return fmt.Errorf("error creating hash file: %w", err)
	}

	_, err = hashFile.WriteString(fmt.Sprintf("%s %s\n", sum, sf.SnapshotFileName))
	if err != nil {
		return fmt.Errorf("error writing hash file: %w", err)
	}

	return nil
}

// ValidateSignature verifies the cryptographic signature of the snapshot file using the provided public key.
// It reads the signature from the signature file and validates it against the snapshot file content.
//
// Parameters:
//   - publicKey: A string containing the PGP public key in armored format used to verify the signature.
//
// Returns:
//   - *openpgp.Entity: The entity (signer) that created the signature if validation is successful.
//   - error: An error if the signature validation fails for any reason, including:
//     * Invalid public key format
//     * Missing signature file
//     * Missing snapshot file
//     * Invalid or tampered signature
//     * Signature created with a different key
//
// The method performs the following steps:
// 1. Parses the provided public key
// 2. Opens the signature file
// 3. Opens the original snapshot file
// 4. Verifies that the signature matches the file content and was created with the corresponding private key
func (sf *SnapshotFile) ValidateSignature(publicKey string) (*openpgp.Entity, error) {
	publicKeyReader := strings.NewReader(publicKey)

	keyRing, err := openpgp.ReadArmoredKeyRing(publicKeyReader)
	if err != nil {
		return nil, fmt.Errorf("error reading armored key ring: %w", err)
	}

	signagureFile, err := os.Open(sf.SignatureFilePath())
	if err != nil {
		return nil, fmt.Errorf("error opening signature file: %w", err)
	}
	defer signagureFile.Close()

	originalFile, err := os.Open(sf.FullPath())
	if err != nil {
		return nil, fmt.Errorf("error opening snapshot file: %w", err)
	}
	defer originalFile.Close()

	signer, err := openpgp.CheckArmoredDetachedSignature(keyRing, originalFile, signagureFile, nil)
	if err != nil {
		return nil, fmt.Errorf("error checking signature: %w", err)
	}

	return signer, nil
}

// ClearFiles removes the snapshot file, hash file, and signature file.
// It ignores any errors that occur during removal.
func (sf *SnapshotFile) ClearFiles() {
	_ = os.Remove(sf.FullPath())
	_ = os.Remove(sf.HashFilePath())
	_ = os.Remove(sf.SignatureFilePath())
}

// MetadataFileName returns the filename for the snapshot's metadata file.
func (sf *SnapshotFile) MetadataFileName() string {
	return "metadata.json"
}

// MetadataFilePath returns the absolute path to the snapshot's metadata file.
func (sf *SnapshotFile) MetadataFilePath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.MetadataFileName())
}

// GetMetadata creates and returns a SnapshotMetadata struct containing
// the snapshot's metadata information.
func (sf *SnapshotFile) GetMetadata() *SnapshotMetadata {
	return &SnapshotMetadata{
		Version:   sf.Version,
		Chain:     sf.Chain,
		Schema:    sf.SchemaName,
		Kind:      sf.Kind,
		Timestamp: sf.CreatedTimestamp.Format(time.RFC3339),
		FileName:  sf.SnapshotFileName,
	}
}

// GenerateAndSaveMetadata creates a metadata file containing information about the snapshot.
// The metadata is saved as a JSON file in the snapshot directory.
// Returns an error if any operation fails.
func (sf *SnapshotFile) GenerateAndSaveMetadata() error {
	metadataFilePath := sf.MetadataFilePath()

	metadata := sf.GetMetadata()
	metadataJson, err := json.MarshalIndent(metadata, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshalling metadata: %w", err)
	}

	metadataFile, err := os.OpenFile(metadataFilePath, os.O_CREATE|os.O_WRONLY, 0775)
	if err != nil {
		return fmt.Errorf("error creating metadata file: %w", err)
	}
	_, err = metadataFile.Write(metadataJson)
	if err != nil {
		return fmt.Errorf("error writing metadata file: %w", err)
	}
	return nil
}

// newSnapshotFile creates a new SnapshotFile instance from an existing snapshot file path.
// It extracts the directory and filename from the provided path.
func newSnapshotFile(snapshotFileName string) *SnapshotFile {
	name := filepath.Base(snapshotFileName)
	dir := filepath.Dir(snapshotFileName)

	return &SnapshotFile{
		Dir:              dir,
		SnapshotFileName: name,
	}
}

// newSnapshotDumpFile creates a new SnapshotFile instance for a snapshot to be created.
// It generates a filename based on the provided parameters and the current timestamp.
func newSnapshotDumpFile(destPath string, chain string, version string, schemaName string, kind Kind) *SnapshotFile {
	// generate date YYYYMMDDhhmmss
	now := time.Now()
	date := now.Format("20060102150405")

	fileName := fmt.Sprintf("sidecar_%s_%s_%s_%s_%s.dump", chain, kind, version, schemaName, date)

	return &SnapshotFile{
		Dir:              destPath,
		SnapshotFileName: fileName,
		CreatedTimestamp: now,
		Chain:            chain,
		Version:          version,
		SchemaName:       schemaName,
		Kind:             string(kind),
	}
}

package snapshot

import (
	"crypto/sha256"
	"fmt"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type SnapshotFile struct {
	Dir              string
	SnapshotFileName string
}

func (sf *SnapshotFile) HashExt() string {
	return "sha256"
}

func (sf *SnapshotFile) SignatureExt() string {
	return "asc"
}

func (sf *SnapshotFile) HashFileName() string {
	return fmt.Sprintf("%s.%s", sf.SnapshotFileName, sf.HashExt())
}

func (sf *SnapshotFile) SignatureFileName() string {
	return fmt.Sprintf("%s.%s", sf.SnapshotFileName, sf.SignatureExt())
}

func (sf *SnapshotFile) FullPath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.SnapshotFileName)
}

func (sf *SnapshotFile) HashFilePath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.HashFileName())
}

func (sf *SnapshotFile) SignatureFilePath() string {
	return fmt.Sprintf("%s/%s", sf.Dir, sf.SignatureFileName())
}

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

func (sf *SnapshotFile) ValidateSignature(publicKey string) error {
	return nil
}

func (sf *SnapshotFile) ClearFiles() {
	_ = os.Remove(sf.FullPath())
	_ = os.Remove(sf.HashFilePath())
	_ = os.Remove(sf.SignatureFilePath())
}

func newSnapshotFile(snapshotFileName string) *SnapshotFile {
	name := filepath.Base(snapshotFileName)
	dir := filepath.Dir(snapshotFileName)

	return &SnapshotFile{
		Dir:              dir,
		SnapshotFileName: name,
	}
}

func newSnapshotDumpFile(destPath string, chain string, version string, schemaName string) *SnapshotFile {
	// generate date YYYYMMDDhhmmss
	date := time.Now().Format("20060102150405")

	fileName := fmt.Sprintf("sidecar_%s_%s_%s_%s.dump", chain, version, schemaName, date)

	return &SnapshotFile{
		Dir:              destPath,
		SnapshotFileName: fileName,
	}
}

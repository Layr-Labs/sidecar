#!/usr/bin/env bash

name=$1

if [[ -z $name ]]; then
    echo "Usage: $0 <migration_name>"
    exit 1
fi

timestamp=$(date +"%Y%m%d%H%M")

migration_name="${timestamp}_${name}"

migrations_dir="./pkg/coreContracts/migrations/${migration_name}"
migration_file="${migrations_dir}/up.go"

mkdir -p $migrations_dir || true

# heredoc that creates a migration go file with an Up function
cat > $migration_file <<EOF
package _${timestamp}_${name}

import (
	"github.com/Layr-Labs/sidecar/internal/config"
    "github.com/Layr-Labs/sidecar/pkg/contractStore"
    "github.com/Layr-Labs/sidecar/pkg/coreContracts/types"
    "go.uber.org/zap"
    "gorm.io/gorm"
)

type ContractMigration struct{}

func (m *ContractMigration) Up(db *gorm.DB, cs contractStore.ContractStore, l *zap.Logger, cfg *config.Config) (*types.MigrationResult, error) {
	return nil, nil
}

func (m *ContractMigration) GetName() string {
	return "${timestamp}_${name}"
}
EOF

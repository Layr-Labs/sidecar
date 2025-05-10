#!/usr/bin/env bash

name=$1

if [[ -z $name ]]; then
    echo "Usage: $0 <job_name>"
    exit 1
fi

timestamp=$(date +"%Y%m%d%H%M")

job_name="${timestamp}_${name}"

migrations_dir="./pkg/sidecar/startupJobs/${job_name}"
migration_file="${migrations_dir}/job.go"

mkdir -p $migrations_dir || true

# heredoc that creates a migration go file with an Up function
cat > $migration_file <<EOF
package _${job_name}

import (
	"context"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/indexer"
	"gorm.io/gorm"
)

type StartupJob struct{}

func (s *StartupJob) Run(
	ctx context.Context,
	ethClient *ethereum.Client,
	indxr *indexer.Indexer,
	grm *gorm.DB,
) error {
	return nil
}

func (s *StartupJob) Name() string {
	return "${job_name}"
}

EOF

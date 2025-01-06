package _202501061613_reindexTestnetForStaterootChange

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

// Up
// In response to the rewards-v2 audit, we needed to change how a couple of model state roots
// were calculated. While this doesnt affect rewards generation, it does affect state roots that
// have already been calculated and stored for preprod and testnet. To ensure that all state roots
// are consistent, we need to prune back to the block where rewards-v2 was first deployed to allow
// rewards to be recaclulated with the new state root calculation.
func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	var blockNumber uint64
	if cfg.Chain == config.Chain_Preprod {
		blockNumber = 2871556
	} else if cfg.Chain == config.Chain_Holesky {
		blockNumber = 2930139
	} else {
		// chain doesnt require pruning
		return nil
	}

	query := `delete from blocks where number >= @blockNumber`

	res := grm.Exec(query, sql.Named("blockNumber", blockNumber))
	if res.Error != nil {
		return res.Error
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202501061613_reindexTestnetForStaterootChange"
}

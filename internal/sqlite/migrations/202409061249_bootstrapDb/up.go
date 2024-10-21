package _202409061249_bootstrapDb

import (
	"fmt"

	"gorm.io/gorm"
)

type SqliteMigration struct {
}

func (m *SqliteMigration) Up(grm *gorm.DB) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS blocks (
			number INTEGER NOT NULL PRIMARY KEY,
			hash text NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			block_time DATETIME NOT NULL,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL
    	)`,
		`CREATE TABLE IF NOT EXISTS transactions (
			block_number INTEGER NOT NULL REFERENCES blocks(number) ON DELETE CASCADE,
			block_time DATETIME NOT NULL,
			transaction_hash TEXT NOT NULL PRIMARY KEY,
			transaction_index INTEGER NOT NULL,
			from_address TEXT NOT NULL,
			to_address TEXT DEFAULT NULL,
			contract_address TEXT DEFAULT NULL,
			bytecode_hash TEXT DEFAULT NULL,
			gas_used INTEGER DEFAULT NULL,
			cumulative_gas_used INTEGER DEFAULT NULL,
			effective_gas_price INTEGER DEFAULT NULL,
			created_at DATETIME DEFAULT current_timestamp,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL,
			UNIQUE(block_number, transaction_hash, transaction_index)
		)`,
		`CREATE TABLE IF NOT EXISTS transaction_logs (
			transaction_hash TEXT NOT NULL REFERENCES transactions(transaction_hash) ON DELETE CASCADE,
			address TEXT NOT NULL,
			arguments TEXT,
			event_name TEXT NOT NULL,
			log_index INTEGER NOT NULL,
			block_number INTEGER NOT NULL REFERENCES blocks(number) ON DELETE CASCADE,
			block_time DATETIME NOT NULL,
			transaction_index INTEGER NOT NULL,
			output_data TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL,
			UNIQUE(transaction_hash, log_index)
    	)`,
		`CREATE TABLE IF NOT EXISTS contracts (
			contract_address TEXT NOT NULL,
			contract_abi TEXT,
			bytecode_hash TEXT DEFAULT NULL,
			verified INTEGER DEFAULT false,
			checked_for_proxy INTEGER DEFAULT 0,
			checked_for_abi INTEGER DEFAULT 0,
			matching_contract_address TEXT DEFAULT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL,
			UNIQUE(contract_address)
		)`,
		`CREATE TABLE IF NOT EXISTS proxy_contracts (
			block_number INTEGER NOT NULL,
			contract_address TEXT NOT NULL REFERENCES contracts(contract_address) ON DELETE CASCADE,
			proxy_contract_address TEXT NOT NULL,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL,
			unique(contract_address, proxy_contract_address, block_number) 
		)`,
		`CREATE TABLE IF NOT EXISTS operator_restaked_strategies (
			block_number INTEGER NOT NULL REFERENCES blocks(number) ON DELETE CASCADE,
			operator TEXT NOT NULL,
			avs TEXT NOT NULL,
			strategy TEXT NOT NULL,
			block_time DATETIME NOT NULL,
			avs_directory_address TEXT,
			created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
			updated_at DATETIME DEFAULT NULL,
			deleted_at DATETIME DEFAULT NULL
		);`,
	}

	for _, query := range queries {
		res := grm.Exec(query)
		if res.Error != nil {
			fmt.Printf("Failed to run migration query: %s - %+v\n", query, res.Error)
			return res.Error
		}
	}
	return nil
}

func (m *SqliteMigration) GetName() string {
	return "202409061249_bootstrapDb"
}

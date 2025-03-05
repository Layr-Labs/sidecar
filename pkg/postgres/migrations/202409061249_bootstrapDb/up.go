package _202409061249_bootstrapDb

import (
	"database/sql"
	"fmt"
	"github.com/Layr-Labs/sidecar/internal/config"

	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS blocks (
			number bigint NOT NULL PRIMARY KEY,
			hash varchar(255) NOT NULL,
			block_time timestamp without time zone NOT NULL,
			created_at timestamp without time zone DEFAULT current_timestamp,
			updated_at timestamp without time zone DEFAULT NULL,
			deleted_at timestamp without time zone
    	)`,
		`CREATE TABLE IF NOT EXISTS transactions (
			block_number bigint NOT NULL,
			transaction_hash character varying(255) NOT NULL,
			transaction_index bigint NOT NULL,
			from_address character varying(255) NOT NULL,
			to_address character varying(255) DEFAULT NULL::character varying,
			contract_address character varying(255) DEFAULT NULL::character varying,
			bytecode_hash character varying(64) DEFAULT NULL::character varying,
			gas_used numeric,
			cumulative_gas_used numeric,
			effective_gas_price numeric,
    		created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
			updated_at timestamp without time zone,
			deleted_at timestamp without time zone,
			UNIQUE(block_number, transaction_hash, transaction_index)
		)`,
		`CREATE TABLE IF NOT EXISTS transaction_logs (
			transaction_hash character varying(255) NOT NULL,
			address character varying(255) NOT NULL,
			arguments jsonb,
			event_name character varying(255) NOT NULL,
			log_index bigint NOT NULL,
			block_number bigint NOT NULL,
			transaction_index integer NOT NULL,
			output_data jsonb,
    		created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
			updated_at timestamp without time zone,
			deleted_at timestamp without time zone,
			UNIQUE(transaction_hash, log_index)
    	)`,
		`CREATE TABLE IF NOT EXISTS contracts (
    		id SERIAL PRIMARY KEY,
			contract_address character varying(255) NOT NULL,
			contract_abi text,
			bytecode_hash character varying(64) DEFAULT NULL::character varying,
			verified boolean DEFAULT false,
			matching_contract_address character varying(255) DEFAULT NULL::character varying,
			checked_for_proxy boolean DEFAULT false NOT NULL,
			checked_for_abi boolean,
    		created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
			updated_at timestamp without time zone,
			deleted_at timestamp without time zone,
			UNIQUE(contract_address)
		)`,
		`CREATE TABLE IF NOT EXISTS proxy_contracts (
			block_number bigint NOT NULL,
			contract_address character varying(255) NOT NULL,
			proxy_contract_address character varying(255) NOT NULL,
			created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP NOT NULL,
			updated_at timestamp without time zone,
			deleted_at timestamp without time zone,
			unique(contract_address, proxy_contract_address, block_number) 
		)`,
		`CREATE TABLE IF NOT EXISTS operator_restaked_strategies (
			block_number bigint NOT NULL,
			operator character varying NOT NULL,
			avs character varying NOT NULL,
			strategy character varying NOT NULL,
			block_time timestamp without time zone NOT NULL,
			avs_directory_address character varying,
			created_at timestamp without time zone DEFAULT CURRENT_TIMESTAMP,
			updated_at timestamp without time zone,
			deleted_at timestamp without time zone
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

func (m *Migration) GetName() string {
	return "202409061249_bootstrapDb"
}

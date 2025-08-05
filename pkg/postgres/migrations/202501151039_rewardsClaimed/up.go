package _202501151039_rewardsClaimed

import (
	"database/sql"

	"github.com/Layr-Labs/sidecar/internal/config"
	"gorm.io/gorm"
)

type Migration struct {
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	query := `
		CREATE TABLE IF NOT EXISTS rewards_claimed (
			root            varchar not null,
			earner          varchar not null,
			claimer         varchar not null,
			recipient       varchar default null,
			token           varchar not null,
			claimed_amount  numeric not null,
			transaction_hash varchar not null,
			block_number     bigint not null,
			log_index        bigint not null,
			unique(transaction_hash, log_index),
		    foreign key (block_number) references blocks(number) on delete cascade
		);	
	`
	res := grm.Exec(query)
	if res.Error != nil {
		return res.Error
	}
	query = `
		insert into rewards_claimed (root, token, claimed_amount, earner, claimer, recipient, transaction_hash, block_number, log_index)
		select
			concat('0x', (
			  SELECT lower(string_agg(lpad(to_hex(elem::int), 2, '0'), ''))
			  FROM jsonb_array_elements_text(tl.output_data->'root') AS elem
			)) AS root,
			lower(tl.output_data->>'token'::text) as token,
			cast(tl.output_data->>'claimedAmount' as numeric) as claimed_amount,
			lower(tl.arguments #>> '{1, Value}') as earner,
			lower(tl.arguments #>> '{2, Value}') as claimer,
			lower(coalesce(tl.arguments #>> '{3, Value}', '')) as recipient,
			tl.transaction_hash,
			tl.block_number,
			tl.log_index
		from transaction_logs as tl
		where
			tl.address = @rewardsCoordinatorAddress
			and tl.event_name = 'RewardsClaimed'
		order by tl.block_number asc
		on conflict do nothing
	`
	contractAddresses := cfg.GetContractsMapForChain()
	res = grm.Exec(query, sql.Named("rewardsCoordinatorAddress", contractAddresses.RewardsCoordinator))
	return res.Error
}

func (m *Migration) GetName() string {
	return "202501151039_rewardsClaimed"
}

package _202410241539_goldTables

import (
	"database/sql"
	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"gorm.io/gorm"
)

type Migration struct {
}

var exludedAddresses = map[config.Chain][]string{
	config.Chain_Preprod: []string{},
	config.Chain_Holesky: []string{},
	config.Chain_Sepolia: []string{},
	config.Chain_Hoodi:   []string{},
	config.Chain_Mainnet: []string{
		"0x3d4eec9f015c9016f5142055a965188d17bba06f",
		"0x56458b6686a033476a9472e6917fca33ce2ba4fa",
		"0xf1dfec3a799a25b9bf0911b03401e01d63915574",
		"0xcc05239823c0873cef85c02f3ba58d0e8398e338",
		"0x8440769d3b4cbb7ef5b04b127213b22dea23a82f",
		"0x35563df2a9ca8f973cc9670dd0ea15ae60b3dbd3",
		"0xcdb68cfc19a9808fd549b8b8506da0dbd5935ef4",
		"0x49aff6e3baf509bfb6f151df159889feeb91f3a0",
		"0x7ff1b597258e67520e7c570534d6c0955f696c4f",
		"0x02179f4af1d7ea53841e3a2230375801ab3cc2f9",
		"0xba59853cd3921550ae0773bb648543d138fc205a",
		"0xca2d1d2b7cf448dbfc8888addb9008d71049561f",
		"0xbc57586af1eaa69732d27a9d9901c6926eb811b7",
		"0xf926ce2998b8c87e8b758ae46b5b4ad043f9a299",
		"0x97c0551954040e958c81444c06c8543cdee64a73",
		"0x8264312120aaba43adff803829e82986149d27ae",
		"0x94c6326a478c1eca0fe09a07754eab89a547e4eb",
		"0x6af265e3741817c6ad4bdad82d3d8976ac9bc3cf",
		"0x26516df208cc2f71ad21e31f8725043ae08180b0",
		"0xe2ff36e97afc1c0f5688d0ff76e7441acb0e68d7",
		"0x64bd86ed5cbe443bdeda20ecfaab15ea66119603",
		"0x2da4aa21033da104df6b58eaa24821be629756ae",
		"0x0db954cf3399c6cfc8f8e1ba7b0f1cd97ccfcee4",
		"0xda02bd9b9bb963dc3ad7f72b3648e380aba36448",
		"0x748b6007c1f4fd09258fc0530cb099bf2d9bf4d1",
		"0x774a78488b3aae1d8ffecd8852adb59fa9f4e0ff",
		"0x33d2a6cfcd67cc62625921105f8bf052d97c29fd",
		"0x6e3e0e5f8dc4c90a7382664e3db63a42b80eb9d7",
		"0xe7d40d9a77caddd8e8b4b484ed14c42f3b8d763a",
		"0x321e71e7ff8ccc9e7f1e7377b6546996bfccc313",
		"0xd1192457d3e392a05031aa33e6efbda3aad45c53",
		"0xbf0aaf43144eca99503860d8c5ac16e0875184f6",
		"0xa2ed4478e543ce5071ab4bbfcc27bc4b68ed983a",
		"0xe9215566641932c6429a66e6c4397ecb00915996",
		"0xa743c746c59bcb5e5305af71462452d75403f353",
		"0x7643ac4c159f6e36d1d20c3b843009fea577dbff",
		"0xc562e53262633effbf0da533e56a792777a2b6c5",
		"0xfade4c25ded89f5a0071dd44d4b3735e703c4b46",
		"0x43667652452e0c5ea936071853f2e78b82d2d902",
		"0x3750a7a3be3c3d4c0aa32282eeb3b64a4b35eb93",
		"0xd09dfe5bc2b11db2f3aca6b7c977b635e9bfd2e0",
		"0x0a4715759339c0f23d455432d8c2bc36abe749cf",
		"0x86bc3b961fb5acda1a60a3ddd13e47b8b8bf5c4c",
		"0x56aeebc8fbf95e7b8c572a74efb40ea9dc8ba78e",
		"0x9de3c8c0bff9b63c2d579f97e977912b99135343",
		"0x140fd7285bfc7bc5dc6e74fc93253d2ee65e9c69",
		"0x6295bbc8ab28c51b5878798976d38ade1017af86",
		"0x4a836a3fc5d75002abf3ab3118609b269f43f677",
	},
}

func (m *Migration) Up(db *sql.DB, grm *gorm.DB, cfg *config.Config) error {
	queries := []string{
		`create table if not exists excluded_addresses (
			address varchar not null PRIMARY KEY,
			network varchar not null,
			description varchar,
			created_at timestamp with time zone default current_timestamp,
			updated_at timestamp with time zone,
			deleted_at timestamp with time zone,
			unique(address)
		)`,
		`create table if not exists gold_table (
			earner varchar not null,
			snapshot date NOT NULL,
			reward_hash varchar not null,
			token varchar not null,
			amount numeric not null
		)`,
	}

	for _, query := range queries {
		if err := grm.Exec(query).Error; err != nil {
			return err
		}
	}
	for chain, addresses := range exludedAddresses {
		rows := make([]*storage.ExcludedAddresses, 0)
		for _, address := range addresses {
			rows = append(rows, &storage.ExcludedAddresses{
				Address:     address,
				Network:     chain.String(),
				Description: "panama fork",
			})
		}
		if len(rows) == 0 {
			continue
		}
		if err := grm.Model(&storage.ExcludedAddresses{}).Create(&rows).Error; err != nil {
			return err
		}
	}
	return nil
}

func (m *Migration) GetName() string {
	return "202410241539_goldTables"
}

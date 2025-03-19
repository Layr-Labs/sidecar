package runtime

import "time"

type SidecarVersions struct {
	Id                       uint64 `gorm:"type:serial"`
	Version                  string
	StateRootBlockLaunchedAt uint64
	CreatedAt                *time.Time
}

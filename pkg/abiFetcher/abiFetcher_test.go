package abiFetcher

import (
	"context"
	"testing"

	"os"

	"github.com/Layr-Labs/sidecar/internal/config"
	"github.com/Layr-Labs/sidecar/internal/logger"
	"github.com/Layr-Labs/sidecar/internal/tests"
	"github.com/Layr-Labs/sidecar/pkg/clients/ethereum"
	"github.com/Layr-Labs/sidecar/pkg/postgres"
	"go.uber.org/zap"
	"gorm.io/gorm"
)


func Test_AbiFetcher(t *testing.T) {
	_, _, l, _, err := setup()

	if err != nil {
		t.Fatal(err)
	}

	baseUrl := "https://tame-fabled-liquid.quiknode.pro/f27d4be93b4d7de3679f5c5ae881233f857407a0/"
	ethConfig := ethereum.DefaultNativeCallEthereumClientConfig()
	ethConfig.BaseUrl = baseUrl

	client := ethereum.NewClient(ethConfig, l)

	t.Run("Test fetching ABI from IPFS", func(t *testing.T) {
		af := NewAbiFetcher(client, l)
		_ = af.FetchAbiFromIPFS(context.Background(), "0xdabdb3cd346b7d5f5779b0b614ede1cc9dcba5b7")
	})
}
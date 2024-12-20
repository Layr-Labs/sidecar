package multicall

import (
	"context"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/accounts/abi/bind"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/stretchr/testify/assert"
)

const ethNodeURL = "https://eth.llamarpc.com/"

func setupClient(t *testing.T) *ethclient.Client {
	client, err := ethclient.Dial(ethNodeURL)
	if err != nil {
		t.Fatalf("Failed to connect to the Ethereum client: %v", err)
	}
	return client
}

func TestMulticallClientCreation(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Test creating a new multicall client
	multicallClient, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, multicallClient)
	assert.Equal(t, multicallClient.Address.Hex(), "0xcA11bde05977b3631167028862bE2a173976CA11") // Default multicall address
}

func TestMulticallGetBalance(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	multicallClient, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, multicallClient)

	// Sample Ethereum address (replace with a valid one for testing)
	testAddress := common.HexToAddress("0x742d35Cc6634C0532925a3b844Bc454e4438f44e")

	// Get balance
	balanceCall := multicallClient.GetBalance(testAddress)
	assert.NotNil(t, balanceCall)

	// Execute the call
	balances, err := DoMany(multicallClient, balanceCall)
	assert.NoError(t, err)
	assert.NotNil(t, balances)

	// Validate the balance (just check if it's non-nil for now)
	assert.NotNil(t, (*balances)[0])
}

func TestMulticallGetBlockNumber(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	multicallClient, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, multicallClient)

	// Get block number
	blockNumberCall := multicallClient.GetBlockNumber()
	assert.NotNil(t, blockNumberCall)

	// Execute the call
	blockNumbers, err := DoMany(multicallClient, blockNumberCall, blockNumberCall)
	assert.NoError(t, err)
	assert.NotNil(t, blockNumbers)
	assert.Equal(t, (*blockNumbers)[0], (*blockNumbers)[1])

	// Validate the block number (should be greater than 0)
	blockNumber := (*blockNumbers)[0]
	assert.NotNil(t, blockNumber)
	assert.True(t, blockNumber.Cmp(big.NewInt(0)) > 0)
}

func TestMulticallCustomCall(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	multicallClient, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, multicallClient)

	// ERC721 abi
	contractAbi, _ := abi.JSON(strings.NewReader(`[{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	contractAddress := common.HexToAddress("0x60E4d786628Fea6478F785A6d7e704777c86a7c6") // MAYC
	methodName := "balanceOf"

	// Create a custom call
	customCall, err := Describe[big.Int](contractAddress, contractAbi, methodName, common.HexToAddress("0x040881CA16C00358E9f02E2373310C2fDaC1a5b8"))
	assert.NoError(t, err)
	assert.NotNil(t, customCall)

	// Execute the custom call
	result, result2, err := Do(multicallClient, customCall, customCall)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, result, result2)

	// Validate the result (for now, just check that it's non-nil)
	assert.NotNil(t, result)
}

func TestMulticallCustomCall_2(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	multicallClient, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, multicallClient)

	// ERC721 abi
	contractAbi, _ := abi.JSON(strings.NewReader(`[{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	contractAddress := common.HexToAddress("0x60E4d786628Fea6478F785A6d7e704777c86a7c6") // MAYC

	// Create a custom call
	ownerCall1, err := Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(0))
	assert.NoError(t, err)
	assert.NotNil(t, ownerCall1)

	ownerCall2, err := Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(1))
	assert.NoError(t, err)
	assert.NotNil(t, ownerCall2)

	badCall, err := Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(10000000000000))
	assert.NoError(t, err)
	assert.NotNil(t, badCall)

	// Execute the custom call
	owners, err := DoMany(multicallClient, ownerCall1, ownerCall2)
	assert.NotNil(t, owners)
	assert.Nil(t, err)
	ownerOne := (*owners)[0]
	ownerTwo := (*owners)[1]

	assert.NotNil(t, ownerTwo)
	assert.True(t, ownerOne.Cmp(common.HexToAddress("0x9056D15C49B19dF52FfaD1E6C11627f035C0C960")) == 0, "Got incorrect owner of token 1")
	assert.True(t, ownerTwo.Cmp(common.HexToAddress("0xAA87190076675dA8D3496Da24B0C3BbfA1e56396")) == 0, "Got incorrect owner of token 2")

	// test that DoMany fails if one call fails.
	owners, err = DoMany(multicallClient, ownerCall1, ownerCall2, badCall)
	assert.Nil(t, owners)
	assert.Error(t, err)
}

func TestDoMany(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	mc, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, mc)

	// ERC721 abi
	contractAbi, _ := abi.JSON(strings.NewReader(`[{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	contractAddress := common.HexToAddress("0x60E4d786628Fea6478F785A6d7e704777c86a7c6") // MAYC

	calls := []*MultiCallMetaData[common.Address]{
		panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(0))),
		panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(1))),
	}

	results, err := DoMany(mc, calls...)
	assert.NoError(t, err)

	ownerOne := (*results)[0]
	ownerTwo := (*results)[1]

	// Validate results
	assert.Equal(t, len(*results), 2)

	assert.True(t, ownerOne.Cmp(common.HexToAddress("0x9056D15C49B19dF52FfaD1E6C11627f035C0C960")) == 0, "Got incorrect owner of token 1")
	assert.True(t, ownerTwo.Cmp(common.HexToAddress("0xAA87190076675dA8D3496Da24B0C3BbfA1e56396")) == 0, "Got incorrect owner of token 2")
}

func TestGiganticMulticallFails(t *testing.T) {
	t.Skip()

	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	mc, err := NewMulticallClient(context.Background(), client, &TMulticallClientOptions{
		MaxBatchSizeBytes: math.MaxUint64 - 1, // one big blob
	})
	assert.NoError(t, err)
	assert.NotNil(t, mc)

	contractAbi, _ := abi.JSON(strings.NewReader(`[{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	contractAddress := common.HexToAddress("0x60E4d786628Fea6478F785A6d7e704777c86a7c6") // MAYC

	numCalls := 5000
	calls := make([]*MultiCallMetaData[common.Address], numCalls)
	for i := range numCalls {
		calls[i] = panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(0)))
	}

	res, err := DoMany(mc, calls...)
	assert.Error(t, err)
	assert.Nil(t, res)
}

func TestDoVariadiac(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	mc, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, mc)

	// ERC721 abi
	contractAbi, _ := abi.JSON(strings.NewReader(`[{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	contractAddress := common.HexToAddress("0x60E4d786628Fea6478F785A6d7e704777c86a7c6") // MAYC
	expectedOwner := common.HexToAddress("0x9056D15C49B19dF52FfaD1E6C11627f035C0C960")

	call := panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(0)))

	o1, o2, o3, err := Do3(mc, call, call, call)
	assert.NoError(t, err)

	assert.True(t, o1.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o2.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o3.Cmp(expectedOwner) == 0, "Got incorrect owner of token")

	o1, o2, o3, o4, err := Do4(mc, call, call, call, call)
	assert.NoError(t, err)

	assert.True(t, o1.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o2.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o3.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o4.Cmp(expectedOwner) == 0, "Got incorrect owner of token")

	o1, o2, o3, o4, o5, err := Do5(mc, call, call, call, call, call)
	assert.NoError(t, err)

	assert.True(t, o1.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o2.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o3.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o4.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o5.Cmp(expectedOwner) == 0, "Got incorrect owner of token")

	o1, o2, o3, o4, o5, o6, err := Do6(mc, call, call, call, call, call, call)
	assert.NoError(t, err)

	assert.True(t, o1.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o2.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o3.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o4.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o5.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
	assert.True(t, o6.Cmp(expectedOwner) == 0, "Got incorrect owner of token")
}

func TestDoMany_Struct(t *testing.T) {
	//  maker vat contract
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	mc, err := NewMulticallClient(context.Background(), client, nil)
	eigenpodAbi, _ := EigenPodMetaData.GetAbi()
	assert.NoError(t, err)
	assert.NotNil(t, mc)

	contractAddress := common.HexToAddress("0x525f092e2715d288afd0850aa18dd5d39a920264") // some example mainnet EigenPod.sol deployment

	emptyPubKey := [48]byte{}
	emptyPubKeyUntyped := emptyPubKey[:]

	calls := []*MultiCallMetaData[IEigenPodValidatorInfo]{
		panicIfError(Describe[IEigenPodValidatorInfo](contractAddress, *eigenpodAbi, "validatorPubkeyToInfo", emptyPubKeyUntyped)),
		panicIfError(Describe[IEigenPodValidatorInfo](contractAddress, *eigenpodAbi, "validatorPubkeyToInfo", emptyPubKeyUntyped)),
		panicIfError(Describe[IEigenPodValidatorInfo](contractAddress, *eigenpodAbi, "validatorPubkeyToInfo", emptyPubKeyUntyped)), //invalid
	}

	res, err := DoMany(mc, calls...)
	assert.NoError(t, err)
	assert.NotNil(t, res)

	assert.Len(t, *res, 3)

	assert.NotNil(t, (*res)[0])
	assert.NotNil(t, (*res)[1])
	assert.NotNil(t, (*res)[2])
}

func TestEdgeCases(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// initializing without an eth client fails.
	mc, err := NewMulticallClient(context.Background(), nil, nil)
	assert.Nil(t, mc)
	assert.Error(t, err)

	// initializing with an override address should work...
	overrideAddr := common.HexToAddress("0x00")
	callOpts := &bind.CallOpts{
		BlockNumber: big.NewInt(0),
	}

	mc, err = NewMulticallClient(context.Background(), client, &TMulticallClientOptions{
		OverrideContractAddress: &overrideAddr,
		MaxBatchSizeBytes:       8192,
		OverrideCallOptions:     callOpts,
	})
	assert.NoError(t, err)
	assert.NotNil(t, mc)
	assert.Equal(t, mc.OverrideCallOptions, callOpts)
	assert.Equal(t, mc.Address, overrideAddr)

	mc, err = NewMulticallClient(context.Background(), client, &TMulticallClientOptions{
		OverrideContractAddress: &overrideAddr,
		OverrideCallOptions:     callOpts,
	})
	assert.NoError(t, err)
	assert.NotNil(t, mc)
	assert.Greater(t, mc.MaxBatchSize, uint64(0)) // batch size should be greater than zero by default.
}

func TestDoManyAllowFailures(t *testing.T) {
	client := setupClient(t)
	defer client.Close()

	// Create multicall client
	mc, err := NewMulticallClient(context.Background(), client, nil)
	assert.NoError(t, err)
	assert.NotNil(t, mc)

	// Real Ethereum addresses for the test (use addresses on your testnet)
	contractAbi, _ := abi.JSON(strings.NewReader(`[{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"approve","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"mint","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"},{"internalType":"bytes","name":"_data","type":"bytes"}],"name":"safeTransferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"to","type":"address"},{"internalType":"bool","name":"approved","type":"bool"}],"name":"setApprovalForAll","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"constant":false,"inputs":[{"internalType":"address","name":"from","type":"address"},{"internalType":"address","name":"to","type":"address"},{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"transferFrom","outputs":[],"payable":false,"stateMutability":"nonpayable","type":"function"},{"inputs":[],"payable":false,"stateMutability":"nonpayable","type":"constructor"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"from","type":"address"},{"indexed":true,"internalType":"address","name":"to","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Transfer","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"approved","type":"address"},{"indexed":true,"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"Approval","type":"event"},{"anonymous":false,"inputs":[{"indexed":true,"internalType":"address","name":"owner","type":"address"},{"indexed":true,"internalType":"address","name":"operator","type":"address"},{"indexed":false,"internalType":"bool","name":"approved","type":"bool"}],"name":"ApprovalForAll","type":"event"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"}],"name":"balanceOf","outputs":[{"internalType":"uint256","name":"","type":"uint256"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"getApproved","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"address","name":"owner","type":"address"},{"internalType":"address","name":"operator","type":"address"}],"name":"isApprovedForAll","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"uint256","name":"tokenId","type":"uint256"}],"name":"ownerOf","outputs":[{"internalType":"address","name":"","type":"address"}],"payable":false,"stateMutability":"view","type":"function"},{"constant":true,"inputs":[{"internalType":"bytes4","name":"interfaceId","type":"bytes4"}],"name":"supportsInterface","outputs":[{"internalType":"bool","name":"","type":"bool"}],"payable":false,"stateMutability":"view","type":"function"}]`))
	contractAddress := common.HexToAddress("0x60E4d786628Fea6478F785A6d7e704777c86a7c6") // MAYC

	// Perform multicalls to get balances
	calls := []*MultiCallMetaData[common.Address]{
		panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(0))),
		panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(1))),
		panicIfError(Describe[common.Address](contractAddress, contractAbi, "ownerOf", big.NewInt(10000000000000))), //invalid
	}

	results, err := DoManyAllowFailures(mc, calls...)
	assert.NoError(t, err)

	// Validate results
	assert.Equal(t, len(*results), 3)

	ownerOne := (*results)[0]
	ownerTwo := (*results)[1]
	ownerThree := (*results)[2]

	assert.True(t, ownerOne.Value.Cmp(common.HexToAddress("0x9056D15C49B19dF52FfaD1E6C11627f035C0C960")) == 0, "Got incorrect owner of token 1")
	assert.True(t, ownerTwo.Value.Cmp(common.HexToAddress("0xAA87190076675dA8D3496Da24B0C3BbfA1e56396")) == 0, "Got incorrect owner of token 2")
	assert.False(t, ownerThree.Success) // request 3 sholud fail
}

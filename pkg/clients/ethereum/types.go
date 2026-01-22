package ethereum

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum/common/hexutil"
)

type (
	EthereumHexString   string
	EthereumQuantity    uint64
	EthereumBigQuantity big.Int
	EthereumBigFloat    big.Float
)

type (
	EthereumBlock struct {
		Hash             EthereumHexString      `json:"hash" validate:"required"`
		ParentHash       EthereumHexString      `json:"parentHash" validate:"required"`
		Number           EthereumQuantity       `json:"number"`
		Timestamp        EthereumQuantity       `json:"timestamp" validate:"required_with=Number"`
		Transactions     []*EthereumTransaction `json:"transactions"`
		Nonce            EthereumHexString      `json:"nonce"`
		Sha3Uncles       EthereumHexString      `json:"sha3Uncles"`
		LogsBloom        EthereumHexString      `json:"logsBloom"`
		TransactionsRoot EthereumHexString      `json:"transactionsRoot"`
		StateRoot        EthereumHexString      `json:"stateRoot"`
		ReceiptsRoot     EthereumHexString      `json:"receiptsRoot"`
		Miner            EthereumHexString      `json:"miner"`
		Difficulty       EthereumHexString      `json:"difficulty"` // Post-merge: prevRandao (256-bit hash)
		TotalDifficulty  EthereumBigQuantity    `json:"totalDifficulty"`
		ExtraData        EthereumHexString      `json:"extraData"`
		Size             EthereumQuantity       `json:"size"`
		GasLimit         EthereumQuantity       `json:"gasLimit"`
		GasUsed          EthereumQuantity       `json:"gasUsed"`
		Uncles           []EthereumHexString    `json:"uncles"`
		// The EIP-1559 base fee for the block, if it exists.
		BaseFeePerGas *EthereumQuantity `json:"baseFeePerGas"`
		// 	ExtraHeader   PolygonHeader     `json:"extraHeader"`
		MixHash EthereumHexString `json:"mixHash"`

		// EIP-4895 introduces new fields in the execution payload
		// https://eips.ethereum.org/EIPS/eip-4895
		// Note that the unit of withdrawal `amount` is in Gwei (1e9 wei).
		// Withdrawals     []*EthereumWithdrawal `json:"withdrawals"`
		WithdrawalsRoot EthereumHexString `json:"withdrawalsRoot"`
	}

	EthereumTransactionAccess struct {
		Address     EthereumHexString   `json:"address"`
		StorageKeys []EthereumHexString `json:"storageKeys"`
	}

	EthereumTransaction struct {
		BlockHash   EthereumHexString   `json:"blockHash"`
		BlockNumber EthereumQuantity    `json:"blockNumber"`
		From        EthereumHexString   `json:"from"`
		Gas         EthereumQuantity    `json:"gas"`
		GasPrice    EthereumBigQuantity `json:"gasPrice"`
		Hash        EthereumHexString   `json:"hash"`
		Input       EthereumHexString   `json:"input"`
		To          EthereumHexString   `json:"to"`
		Index       EthereumQuantity    `json:"transactionIndex"`
		Value       EthereumBigQuantity `json:"value"`
		Nonce       EthereumQuantity    `json:"nonce"`
		V           EthereumHexString   `json:"v"`
		R           EthereumHexString   `json:"r"`
		S           EthereumHexString   `json:"s"`

		// The EIP-155 related fields
		ChainId *EthereumQuantity `json:"chainId"`
		// The EIP-2718 type of the transaction
		Type EthereumQuantity `json:"type"`
		// The EIP-1559 related fields
		MaxFeePerGas         *EthereumQuantity             `json:"maxFeePerGas"`
		MaxPriorityFeePerGas *EthereumQuantity             `json:"maxPriorityFeePerGas"`
		AccessList           *[]*EthereumTransactionAccess `json:"accessList"`
		Mint                 *EthereumBigQuantity          `json:"mint"`

		// Deposit transaction fields for Optimism and Base.
		SourceHash EthereumHexString `json:"sourceHash"`
		IsSystemTx bool              `json:"isSystemTx"`
	}

	EthereumTransactionReceipt struct {
		TransactionHash   EthereumHexString   `json:"transactionHash"`
		TransactionIndex  EthereumQuantity    `json:"transactionIndex"`
		BlockHash         EthereumHexString   `json:"blockHash"`
		BlockNumber       EthereumQuantity    `json:"blockNumber"`
		From              EthereumHexString   `json:"from"`
		To                EthereumHexString   `json:"to"`
		CumulativeGasUsed EthereumQuantity    `json:"cumulativeGasUsed"`
		GasUsed           EthereumQuantity    `json:"gasUsed"`
		ContractAddress   EthereumHexString   `json:"contractAddress"`
		Logs              []*EthereumEventLog `json:"logs"`
		LogsBloom         EthereumHexString   `json:"logsBloom"`
		Root              EthereumHexString   `json:"root"`
		Status            *EthereumQuantity   `json:"status"`
		Type              EthereumQuantity    `json:"type"`
		EffectiveGasPrice *EthereumQuantity   `json:"effectiveGasPrice"`

		// Not part of the standard receipt payload, but added for convenience
		ContractBytecode EthereumHexString `json:"contractBytecode"`
	}

	EthereumEventLog struct {
		Removed          bool                `json:"removed"`
		LogIndex         EthereumQuantity    `json:"logIndex"`
		TransactionHash  EthereumHexString   `json:"transactionHash"`
		TransactionIndex EthereumQuantity    `json:"transactionIndex"`
		BlockHash        EthereumHexString   `json:"blockHash"`
		BlockNumber      EthereumQuantity    `json:"blockNumber"`
		Address          EthereumHexString   `json:"address"`
		Data             EthereumHexString   `json:"data"`
		Topics           []EthereumHexString `json:"topics"`
	}
)

func HashBytecode(bytecode string) string {
	hash := sha256.Sum256([]byte(bytecode))

	return fmt.Sprintf("%x", hash)
}

func (et *EthereumTransactionReceipt) GetBytecodeHash() string {
	return HashBytecode(et.ContractBytecode.Value())
}

func (r *EthereumTransactionReceipt) GetTargetAddress() EthereumHexString {
	contractAddress := EthereumHexString("")
	if r.To.Value() != "" {
		contractAddress = r.To
	} else if r.ContractAddress.Value() != "" {
		contractAddress = r.ContractAddress
	}
	return contractAddress
}

func (v EthereumHexString) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%s"`, v)
	return []byte(s), nil
}

func (v *EthereumHexString) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return fmt.Errorf("failed to unmarshal EthereumHexString: %w", err)
	}
	s = strings.ToLower(s)

	*v = EthereumHexString(s)
	return nil
}

func (v EthereumHexString) Value() string {
	return string(v)
}

func (v EthereumQuantity) MarshalJSON() ([]byte, error) {
	s := fmt.Sprintf(`"%s"`, hexutil.EncodeUint64(uint64(v)))
	return []byte(s), nil
}

func (v *EthereumQuantity) UnmarshalJSON(input []byte) error {
	if len(input) > 0 && input[0] != '"' {
		var i uint64
		if err := json.Unmarshal(input, &i); err != nil {
			return fmt.Errorf("failed to unmarshal EthereumQuantity into uint64: %w", err)
		}

		*v = EthereumQuantity(i)
		return nil
	}

	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return fmt.Errorf("failed to unmarshal EthereumQuantity into string: %w", err)
	}

	if s == "" {
		*v = 0
		return nil
	}

	i, err := hexutil.DecodeUint64(s)
	if err != nil {
		return fmt.Errorf("failed to decode EthereumQuantity %v: %w", s, err)
	}

	*v = EthereumQuantity(i)
	return nil
}

func (v EthereumQuantity) Value() uint64 {
	return uint64(v)
}

func (v EthereumQuantity) BigInt() *big.Int {
	return big.NewInt(int64(v))
}

func (v EthereumBigQuantity) MarshalJSON() ([]byte, error) {
	bi := big.Int(v)
	s := fmt.Sprintf(`"%s"`, hexutil.EncodeBig(&bi))
	return []byte(s), nil
}

func (v *EthereumBigQuantity) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return fmt.Errorf("failed to unmarshal EthereumBigQuantity: %w", err)
	}

	if s == "" {
		*v = EthereumBigQuantity{}
		return nil
	}

	i, err := hexutil.DecodeBig(s)
	if err != nil {
		return fmt.Errorf("failed to decode EthereumBigQuantity %v: %w", s, err)
	}

	*v = EthereumBigQuantity(*i)
	return nil
}

func (v EthereumBigQuantity) Value() string {
	i := big.Int(v)
	return i.String()
}

func (v EthereumBigQuantity) Uint64() (uint64, error) {
	i := big.Int(v)
	if !i.IsUint64() {
		return 0, fmt.Errorf("failed to parse EthereumBigQuantity to uint64 %v", v.Value())
	}
	return i.Uint64(), nil
}

func (v EthereumBigFloat) MarshalJSON() ([]byte, error) {
	bf := big.Float(v)
	s := fmt.Sprintf(`"%s"`, bf.String())
	return []byte(s), nil
}

func (v *EthereumBigFloat) UnmarshalJSON(input []byte) error {
	var s string
	if err := json.Unmarshal(input, &s); err != nil {
		return fmt.Errorf("failed to unmarshal EthereumBigFloat: %w", err)
	}

	if s == "" {
		*v = EthereumBigFloat{}
		return nil
	}

	scalar := new(big.Float)
	scalar, ok := scalar.SetString(s)
	if !ok {
		return fmt.Errorf("cannot parse EthereumBigFloat")
	}

	*v = EthereumBigFloat(*scalar)
	return nil
}

func (v EthereumBigFloat) Value() string {
	f := big.Float(v)
	return f.String()
}

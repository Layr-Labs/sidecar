package eventFilter

import (
	"encoding/json"
	"fmt"
	"github.com/Layr-Labs/sidecar/pkg/storage"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"testing"
)

// Example of adding a new filterable type
type CustomEvent struct {
	ID       string `filter:"true"`
	Count    uint64 `filter:"true"`
	Metadata string // Not filterable
}

func createRegistry() *FilterableRegistry {
	reg := NewFilterableRegistry()
	_ = reg.RegisterType(&storage.Transaction{})
	_ = reg.RegisterType(&storage.Block{})
	_ = reg.RegisterType(&storage.TransactionLog{})
	_ = reg.RegisterType(&CustomEvent{})
	return reg
}

func TestConditionEvaluation(t *testing.T) {
	reg := createRegistry()

	tests := []struct {
		name     string
		filter   *Condition
		entity   interface{}
		expected bool
		hasError bool
	}{
		{
			name: "equals_match",
			filter: &Condition{
				Field:   "Number",
				Op:      Equals,
				Value:   uint64(1000),
				EntType: "Block",
			},
			entity: &storage.Block{
				Number: 1000,
				Hash:   "0x123",
			},
			expected: true,
			hasError: false,
		},
		{
			name: "equals_no_match",
			filter: &Condition{
				Field:   "Number",
				Op:      Equals,
				Value:   uint64(1000),
				EntType: "Block",
			},
			entity: &storage.Block{
				Number: 1001,
				Hash:   "0x123",
			},
			expected: false,
			hasError: false,
		},
		{
			name: "greater_than_match",
			filter: &Condition{
				Field:   "Number",
				Op:      GreaterThan,
				Value:   uint64(1000),
				EntType: "Block",
			},
			entity: &storage.Block{
				Number: 1001,
				Hash:   "0x123",
			},
			expected: true,
			hasError: false,
		},
		{
			name: "invalid_entity_type",
			filter: &Condition{
				Field:   "Number",
				Op:      Equals,
				Value:   uint64(1000),
				EntType: "Block",
			},
			entity:   &storage.Transaction{}, // Wrong type
			expected: false,
			hasError: true,
		},
		{
			name: "invalid_field",
			filter: &Condition{
				Field:   "NonexistentField",
				Op:      Equals,
				Value:   uint64(1000),
				EntType: "Block",
			},
			entity: &storage.Block{
				Number: 1000,
				Hash:   "0x123",
			},
			expected: false,
			hasError: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.filter.Evaluate(tt.entity, reg)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestLogicalOperations(t *testing.T) {
	reg := createRegistry()

	block := &storage.Block{
		Number: 1000,
		Hash:   "0x123",
	}

	tests := []struct {
		name     string
		filter   Filter
		expected bool
		hasError bool
	}{
		{
			name: "and_both_true",
			filter: &And{
				Filters: []Filter{
					&Condition{
						Field:   "Number",
						Op:      Equals,
						Value:   uint64(1000),
						EntType: "Block",
					},
					&Condition{
						Field:   "Hash",
						Op:      Equals,
						Value:   "0x123",
						EntType: "Block",
					},
				},
			},
			expected: true,
			hasError: false,
		},
		{
			name: "and_one_false",
			filter: &And{
				Filters: []Filter{
					&Condition{
						Field:   "Number",
						Op:      Equals,
						Value:   uint64(1000),
						EntType: "Block",
					},
					&Condition{
						Field:   "Hash",
						Op:      Equals,
						Value:   "0x456",
						EntType: "Block",
					},
				},
			},
			expected: false,
			hasError: false,
		},
		{
			name: "or_one_true",
			filter: &Or{
				Filters: []Filter{
					&Condition{
						Field:   "Number",
						Op:      Equals,
						Value:   uint64(1000),
						EntType: "Block",
					},
					&Condition{
						Field:   "Hash",
						Op:      Equals,
						Value:   "0x456",
						EntType: "Block",
					},
				},
			},
			expected: true,
			hasError: false,
		},
		{
			name: "or_all_false",
			filter: &Or{
				Filters: []Filter{
					&Condition{
						Field:   "Number",
						Op:      Equals,
						Value:   uint64(999),
						EntType: "Block",
					},
					&Condition{
						Field:   "Hash",
						Op:      Equals,
						Value:   "0x456",
						EntType: "Block",
					},
				},
			},
			expected: false,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := tt.filter.Evaluate(block, reg)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.expected, result)
			}
		})
	}
}

func TestJSONSerialization(t *testing.T) {
	reg := createRegistry()

	original := &And{
		Filters: []Filter{
			&Condition{
				Field:   "BlockNumber",
				Op:      GreaterThan,
				Value:   1000,
				EntType: "Transaction",
			},
			&Or{
				Filters: []Filter{
					&Condition{
						Field:   "FromAddress",
						Op:      Equals,
						Value:   "0x123",
						EntType: "Transaction",
					},
					&Condition{
						Field:   "ToAddress",
						Op:      Equals,
						Value:   "0x456",
						EntType: "Transaction",
					},
				},
			},
		},
	}

	// Marshal to JSON
	jsonData, err := json.MarshalIndent(original, "", "\t")
	require.NoError(t, err)

	// Unmarshal back to filter
	var reconstructed And
	err = json.Unmarshal(jsonData, &reconstructed)
	require.NoError(t, err)

	// Test both filters against the same transaction
	tx := &storage.Transaction{
		BlockNumber: 1500,
		FromAddress: "0x123",
	}

	originalResult, err := original.Evaluate(tx, reg)
	require.NoError(t, err)

	reconstructedResult, err := reconstructed.Evaluate(tx, reg)
	require.NoError(t, err)

	assert.Equal(t, originalResult, reconstructedResult)
}

func TestFilterBuilder(t *testing.T) {
	reg := createRegistry()

	tests := []struct {
		name     string
		builder  func() Filter
		tx       *storage.Transaction
		expected bool
		hasError bool
	}{
		{
			name: "simple_condition",
			builder: func() Filter {
				builder := NewFilterBuilder("Transaction")
				return builder.Condition("BlockNumber", GreaterThan, uint64(1000))
			},
			tx: &storage.Transaction{
				BlockNumber: 1500,
			},
			expected: true,
			hasError: false,
		},
		{
			name: "and_conditions",
			builder: func() Filter {
				builder := NewFilterBuilder("Transaction")
				return builder.And(
					builder.Condition("BlockNumber", GreaterThan, uint64(1000)),
					builder.Condition("FromAddress", Equals, "0x123"),
				)
			},
			tx: &storage.Transaction{
				BlockNumber: 1500,
				FromAddress: "0x123",
			},
			expected: true,
			hasError: false,
		},
		{
			name: "with_group",
			builder: func() Filter {
				builder := NewFilterBuilder("Transaction")
				return builder.And(
					builder.Condition("BlockNumber", GreaterThan, uint64(1000)),
					builder.Or(
						builder.Condition("FromAddress", Equals, "0x123"),
						builder.Condition("ToAddress", Equals, "0x456"),
					),
				)
			},
			tx: &storage.Transaction{
				BlockNumber: 1500,
				FromAddress: "0x123",
			},
			expected: true,
			hasError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			filter := tt.builder()
			result, err := filter.Evaluate(tt.tx, reg)
			if tt.hasError {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
				filterJson, _ := json.MarshalIndent(filter, "", "\t")
				fmt.Printf("Filter: %v\n", string(filterJson))
				fmt.Printf("Result: %v\n", result)
				assert.Equal(t, tt.expected, result, tt.name)
			}
		})
	}
}

func TestOperatorString(t *testing.T) {
	tests := []struct {
		operator Operator
		expected string
	}{
		{Equals, "eq"},
		{NotEquals, "ne"},
		{GreaterThan, "gt"},
		{LessThan, "lt"},
		{GreaterEqual, "gte"},
		{LessEqual, "lte"},
		{Contains, "contains"},
		{NotContains, "notContains"},
		{Operator(999), "unknown"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.operator.String())
		})
	}
}

func TestParseOperator(t *testing.T) {
	tests := []struct {
		input    string
		expected Operator
	}{
		{"eq", Equals},
		{"ne", NotEquals},
		{"gt", GreaterThan},
		{"lt", LessThan},
		{"gte", GreaterEqual},
		{"lte", LessEqual},
		{"contains", Contains},
		{"notContains", NotContains},
		{"invalid", Equals}, // Default case
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			assert.Equal(t, tt.expected, ParseOperator(tt.input))
		})
	}
}

func TestEntityTypeString(t *testing.T) {
	tests := []struct {
		entityType EntityType
		expected   string
	}{
		{"Block", "Block"},
		{"Transaction", "Transaction"},
		{"TransactionLog", "TransactionLog"},
		{EntityType("Foo"), "Foo"},
	}

	for _, tt := range tests {
		t.Run(tt.expected, func(t *testing.T) {
			assert.Equal(t, tt.expected, tt.entityType.String())
		})
	}
}

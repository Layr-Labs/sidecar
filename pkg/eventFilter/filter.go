package eventFilter

import (
	"fmt"
	"reflect"
	"strings"
)

const (
	Condition_OR        = "or"
	Condition_AND       = "and"
	Condition_Condition = "condition"
)

type Filterable interface {
	GetFilterableField(field string) (interface{}, error)
	GetFilterableFields() []FilterableField
}

type FilterableField struct {
	Name  string
	Type  reflect.Type
	Value func(interface{}) (interface{}, error)
}

// Operator represents comparison operations
type Operator int

const (
	Equals Operator = iota
	NotEquals
	GreaterThan
	LessThan
	GreaterEqual
	LessEqual
	Contains
	NotContains
)

func (o Operator) String() string {
	switch o {
	case Equals:
		return "eq"
	case NotEquals:
		return "ne"
	case GreaterThan:
		return "gt"
	case LessThan:
		return "lt"
	case GreaterEqual:
		return "gte"
	case LessEqual:
		return "lte"
	case Contains:
		return "contains"
	case NotContains:
		return "notContains"
	default:
		return "unknown"
	}
}

func ParseOperator(s string) Operator {
	switch strings.ToLower(s) {
	case "eq":
		return Equals
	case "ne":
		return NotEquals
	case "gt":
		return GreaterThan
	case "lt":
		return LessThan
	case "gte":
		return GreaterEqual
	case "lte":
		return LessEqual
	case "contains":
		return Contains
	case "notcontains":
		return NotContains
	default:
		return Equals // default to Equals, could also return error
	}
}

// Filter is the interface that all filter types must implement
type Filter interface {
	Evaluate(entity interface{}, registry *FilterableRegistry) (bool, error)
	Type() string
	ToFilterJSON() (FilterJSON, error)
}

// FilterJSON represents the JSON structure for any filter type
type FilterJSON struct {
	Type     string       `json:"type"`
	Field    string       `json:"field,omitempty"`
	Operator string       `json:"operator,omitempty"`
	Value    interface{}  `json:"value,omitempty"`
	Filters  []FilterJSON `json:"filters,omitempty"`
	EntType  string       `json:"entityType,omitempty"`
}

func compare(fieldValue interface{}, op Operator, filterValue interface{}) (bool, error) {
	switch op {
	case Equals:
		return equals(fieldValue, filterValue)
	case NotEquals:
		eq, err := equals(fieldValue, filterValue)
		return !eq, err
	case GreaterThan:
		return compareOrdered(fieldValue, filterValue, true, false)
	case LessThan:
		return compareOrdered(fieldValue, filterValue, false, false)
	case GreaterEqual:
		return compareOrdered(fieldValue, filterValue, true, true)
	case LessEqual:
		return compareOrdered(fieldValue, filterValue, false, true)
	case Contains:
		return contains(fieldValue, filterValue)
	case NotContains:
		c, err := contains(fieldValue, filterValue)
		return !c, err
	default:
		return false, fmt.Errorf("unsupported operator: %v", op)
	}
}

func equals(a, b interface{}) (bool, error) {
	switch v := a.(type) {
	case uint64:
		bVal, ok := b.(uint64)
		if !ok {
			return false, fmt.Errorf("type mismatch: %T != %T", a, b)
		}
		return v == bVal, nil
	case string:
		bVal, ok := b.(string)
		if !ok {
			return false, fmt.Errorf("type mismatch: %T != %T", a, b)
		}
		return v == bVal, nil
	default:
		return false, fmt.Errorf("unsupported type for equals comparison: %T", a)
	}
}

func compareOrdered(fieldValue, filterValue interface{}, greater, equal bool) (bool, error) {
	va := reflect.ValueOf(fieldValue)

	if va.String() == "string" {
		bVal, ok := filterValue.(string)
		if !ok {
			return false, fmt.Errorf("type mismatch: %T != %T", fieldValue, filterValue)
		}
		comp := strings.Compare(fieldValue.(string), bVal)
		if equal && comp == 0 {
			return true, nil
		}
		if greater {
			return comp > 0, nil
		}
		return comp < 0, nil
	}

	if isNumeric(va) {
		res, err := compareNumerics(fieldValue, filterValue)

		// if fieldValue >= filterValue
		if greater && equal {
			return res >= 0, err
		}

		// if fieldValue > fiterValue
		if greater && !equal {
			return res > 0, err
		}

		// if fieldValue < filterValue
		if !greater && !equal {
			return res < 0, err
		}

		if !greater && equal {
			return res <= 0, err
		}
		// if fieldValue == filterValue
		return res == 0, err
	}

	return false, fmt.Errorf("unsupported type for ordered comparison: %T", fieldValue)
}

func contains(a, b interface{}) (bool, error) {
	str, ok := a.(string)
	if !ok {
		return false, fmt.Errorf("contains operator requires string field, got: %T", a)
	}

	pattern, ok := b.(string)
	if !ok {
		return false, fmt.Errorf("contains operator requires string pattern, got: %T", b)
	}

	return strings.Contains(str, pattern), nil
}

// filterToJSON converts a Filter to its JSON representation
//
//nolint:unused
func filterToJSON(f Filter) (FilterJSON, error) {
	switch v := f.(type) {
	case *Condition:
		return FilterJSON{
			Type:     v.Type(),
			Field:    v.Field,
			Operator: v.Op.String(),
			Value:    v.Value,
			EntType:  v.EntType.String(),
		}, nil
	case *And:
		filters := make([]FilterJSON, len(v.Filters))
		for i, f := range v.Filters {
			var err error
			filter, err := filterToJSON(f)
			if err != nil {
				return FilterJSON{}, err
			}
			filters[i] = filter
		}
		return FilterJSON{
			Type:    v.Type(),
			Filters: filters,
		}, nil
	case *Or:
		filters := make([]FilterJSON, len(v.Filters))
		for i, f := range v.Filters {
			var err error
			filter, err := filterToJSON(f)
			if err != nil {
				return FilterJSON{}, err
			}
			filters[i] = filter
		}
		return FilterJSON{
			Type:    v.Type(),
			Filters: filters,
		}, nil
	default:
		return FilterJSON{}, fmt.Errorf("unknown filter type: %T", f)
	}
}

// ParseFilter creates a Filter from a FilterJSON structure
func ParseFilter(f FilterJSON) (Filter, error) {
	switch f.Type {
	case Condition_Condition:
		return &Condition{
			Field:   f.Field,
			Op:      ParseOperator(f.Operator),
			Value:   f.Value,
			EntType: EntityType(f.EntType),
		}, nil
	case Condition_AND:
		filters := make([]Filter, len(f.Filters))
		for i, filter := range f.Filters {
			var err error
			filters[i], err = ParseFilter(filter)
			if err != nil {
				return nil, err
			}
		}
		return &And{Filters: filters}, nil
	case Condition_OR:
		filters := make([]Filter, len(f.Filters))
		for i, filter := range f.Filters {
			var err error
			filters[i], err = ParseFilter(filter)
			if err != nil {
				return nil, err
			}
		}
		return &Or{Filters: filters}, nil
	default:
		return nil, fmt.Errorf("unknown filter type: %s", f.Type)
	}
}

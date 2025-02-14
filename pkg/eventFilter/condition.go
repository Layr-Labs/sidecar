package eventFilter

import (
	"encoding/json"
	"fmt"
)

// Condition represents a single filter condition
type Condition struct {
	Field   string      `json:"-"`
	Op      Operator    `json:"-"`
	Value   interface{} `json:"-"`
	EntType EntityType  `json:"-"`
}

func (c *Condition) Type() string {
	return "condition"
}

func (c *Condition) ToFilterJSON() (FilterJSON, error) {
	return FilterJSON{
		Type:     c.Type(),
		Field:    c.Field,
		Operator: c.Op.String(),
		Value:    c.Value,
		EntType:  c.EntType.String(),
	}, nil
}

func (c *Condition) MarshalJSON() ([]byte, error) {
	fj, err := c.ToFilterJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(fj)
}

func (c *Condition) UnmarshalJSON(data []byte) error {
	var f FilterJSON
	if err := json.Unmarshal(data, &f); err != nil {
		return err
	}

	c.Field = f.Field
	c.Value = f.Value
	c.EntType = EntityType(f.EntType)
	c.Op = ParseOperator(f.Operator)

	return nil
}

func (c *Condition) Evaluate(entity interface{}, registry *FilterableRegistry) (bool, error) {
	// Validate entity type
	if err := registry.ValidateEntityType(c.EntType, entity); err != nil {
		return false, err
	}

	fieldValue, err := registry.GetFilterableField(entity, c.Field)
	if err != nil {
		return false, err
	}

	return compare(fieldValue, c.Op, c.Value)
}

// And combines multiple filters with AND logic
type And struct {
	Filters []Filter
}

func (a *And) ToFilterJSON() (FilterJSON, error) {
	filters := make([]FilterJSON, len(a.Filters))
	for i, f := range a.Filters {
		fj, err := f.ToFilterJSON()
		if err != nil {
			return FilterJSON{}, err
		}
		filters[i] = fj
	}

	return FilterJSON{
		Type:    a.Type(),
		Filters: filters,
	}, nil
}

func (a *And) MarshalJSON() ([]byte, error) {
	fj, err := a.ToFilterJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(fj)
}

func (a *And) UnmarshalJSON(data []byte) error {
	var filterJSON FilterJSON
	if err := json.Unmarshal(data, &filterJSON); err != nil {
		return err
	}

	filters := make([]Filter, len(filterJSON.Filters))
	for i, f := range filterJSON.Filters {
		filter, err := ParseFilter(f)
		if err != nil {
			return err
		}
		filters[i] = filter
	}

	a.Filters = filters
	return nil
}

func (a *And) Evaluate(entity interface{}, registry *FilterableRegistry) (bool, error) {
	for _, filter := range a.Filters {
		result, err := filter.Evaluate(entity, registry)
		fmt.Printf("And eval: [] [] %+v\n", result)
		if err != nil {
			return false, err
		}
		if !result {
			return false, nil
		}
	}
	return true, nil
}

func (a *And) Type() string {
	return "and"
}

// Or combines multiple filters with OR logic
type Or struct {
	Filters []Filter
}

func (o *Or) ToFilterJSON() (FilterJSON, error) {
	filters := make([]FilterJSON, len(o.Filters))
	for i, f := range o.Filters {
		fj, err := f.ToFilterJSON()
		if err != nil {
			return FilterJSON{}, err
		}
		filters[i] = fj
	}

	return FilterJSON{
		Type:    o.Type(),
		Filters: filters,
	}, nil
}

func (o *Or) MarshalJSON() ([]byte, error) {
	fj, err := o.ToFilterJSON()
	if err != nil {
		return nil, err
	}
	return json.Marshal(fj)
}

func (o *Or) UnmarshalJSON(data []byte) error {
	var filterJSON FilterJSON
	if err := json.Unmarshal(data, &filterJSON); err != nil {
		return err
	}

	filters := make([]Filter, len(filterJSON.Filters))
	for i, f := range filterJSON.Filters {
		filter, err := ParseFilter(f)
		if err != nil {
			return err
		}
		filters[i] = filter
	}

	o.Filters = filters
	return nil
}

func (o *Or) Type() string {
	return Condition_OR
}

func (o *Or) Evaluate(entity interface{}, registry *FilterableRegistry) (bool, error) {
	fmt.Printf("Or list length: %+v\n", len(o.Filters))
	for _, filter := range o.Filters {
		result, err := filter.Evaluate(entity, registry)
		fmt.Printf("Or eval: err: %+v - %+v\n", err, result)
		if err != nil {
			return false, err
		}
		if result {
			return true, nil
		}
	}
	return false, nil
}

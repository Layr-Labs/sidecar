package eventFilter

type FilterBuilder struct {
	entType EntityType
}

func NewFilterBuilder(entType EntityType) *FilterBuilder {
	return &FilterBuilder{
		entType: entType,
	}
}

func (b *FilterBuilder) Condition(field string, op Operator, value interface{}) Filter {
	return &Condition{
		Field:   field,
		Op:      op,
		Value:   value,
		EntType: b.entType,
	}
}

func (b *FilterBuilder) And(conditions ...Filter) Filter {
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return &And{Filters: conditions}
}

func (b *FilterBuilder) Or(conditions ...Filter) Filter {
	if len(conditions) == 0 {
		return nil
	}
	if len(conditions) == 1 {
		return conditions[0]
	}
	return &Or{Filters: conditions}
}

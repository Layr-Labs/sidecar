package eventFilter

import (
	"fmt"
	"reflect"
	"strings"
)

type EntityType string

func (e EntityType) String() string {
	return string(e)
}

func ParseEntityType(s string) EntityType {
	return EntityType(strings.ToLower(s))
}

type FilterableRegistry struct {
	// Use reflect.Type as key since it uniquely identifies a type
	fields map[reflect.Type][]FilterableField
}

// NewFilterableRegistry creates a new registry
func NewFilterableRegistry() *FilterableRegistry {
	return &FilterableRegistry{
		fields: make(map[reflect.Type][]FilterableField),
	}
}

// RegisterType analyzes a type and registers its filterable fields
func (r *FilterableRegistry) RegisterType(example interface{}) error {
	t := reflect.TypeOf(example)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	var fields []FilterableField
	for i := 0; i < t.NumField(); i++ {
		field := t.Field(i)
		if field.Tag.Get("filter") == "true" {
			idx := i
			fields = append(fields, FilterableField{
				Name: field.Name,
				Type: field.Type,
				Value: func(e interface{}) (interface{}, error) {
					v := reflect.ValueOf(e)
					if v.Kind() == reflect.Ptr {
						v = v.Elem()
					}
					return v.Field(idx).Interface(), nil
				},
			})
		}
	}

	r.fields[t] = fields
	return nil
}

func (r *FilterableRegistry) ValidateEntityType(entityType EntityType, entity interface{}) error {
	actualType := reflect.TypeOf(entity)
	if actualType.Kind() == reflect.Ptr {
		actualType = actualType.Elem()
	}

	if string(entityType) != actualType.Name() {
		return fmt.Errorf("expected %s type, got %s", entityType, actualType.Name())
	}

	// Also verify the type is registered
	if _, ok := r.fields[actualType]; !ok {
		return fmt.Errorf("type %s is not registered", actualType.Name())
	}

	return nil
}

// GetFilterableFields returns the filterable fields for a given type
func (r *FilterableRegistry) GetFilterableFields(entity interface{}) ([]FilterableField, error) {
	t := reflect.TypeOf(entity)
	if t.Kind() == reflect.Ptr {
		t = t.Elem()
	}

	fields, ok := r.fields[t]
	if !ok {
		return nil, fmt.Errorf("type %v not registered", t)
	}
	return fields, nil
}

// GetFilterableField returns a specific field value
func (r *FilterableRegistry) GetFilterableField(entity interface{}, fieldName string) (interface{}, error) {
	fields, err := r.GetFilterableFields(entity)
	if err != nil {
		return nil, err
	}

	for _, field := range fields {
		if field.Name == fieldName {
			return field.Value(entity)
		}
	}
	return nil, fmt.Errorf("field %s not found", fieldName)
}

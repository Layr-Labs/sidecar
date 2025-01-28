package eventFilter

import (
	"fmt"
	"math/big"
	"reflect"
)

// compareNumerics compares two numeric values and returns -1, 0, or 1 if a < b, a == b, or a > b respectively.
// Rather than trying to deal with upcasting or downcasting numbers, we convert both to big.Float and compare those
// since big.Float can handle arbitrary precision.
func compareNumerics(a, b interface{}) (int, error) {
	// Convert both to big.Float
	fa := new(big.Float)
	fb := new(big.Float)

	// Set values from interface{}
	switch v := a.(type) {
	case int, int8, int16, int32, int64:
		fa.SetInt64(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		fa.SetUint64(reflect.ValueOf(v).Uint())
	case float32, float64:
		fa.SetFloat64(reflect.ValueOf(v).Float())
	default:
		return 0, fmt.Errorf("unsupported type for a: %T", a)
	}

	switch v := b.(type) {
	case int, int8, int16, int32, int64:
		fb.SetInt64(reflect.ValueOf(v).Int())
	case uint, uint8, uint16, uint32, uint64:
		fb.SetUint64(reflect.ValueOf(v).Uint())
	case float32, float64:
		fb.SetFloat64(reflect.ValueOf(v).Float())
	default:
		return 0, fmt.Errorf("unsupported type for b: %T", b)
	}

	return fa.Cmp(fb), nil
}

func isNumeric(v reflect.Value) bool {
	switch v.Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Float32, reflect.Float64:
		return true
	default:
		return false
	}
}

package utils

// Map applies a transformation function to each element of a slice and returns a new slice
// with the transformed values. This is a generic implementation of the map higher-order function.
//
// Type Parameters:
//   - A: The type of elements in the input slice
//   - B: The type of elements in the output slice
//
// Parameters:
//   - coll: The input slice to transform
//   - mapper: Function that transforms each element and receives the element's index
//
// Returns:
//   - []B: A new slice containing the transformed elements
func Map[A any, B any](coll []A, mapper func(i A, index uint64) B) []B {
	out := make([]B, len(coll))
	for i, item := range coll {
		out[i] = mapper(item, uint64(i))
	}
	return out
}

// Filter creates a new slice containing only the elements from the input slice
// that satisfy the provided criteria function.
//
// Type Parameters:
//   - A: The type of elements in the slice
//
// Parameters:
//   - coll: The input slice to filter
//   - criteria: Function that determines whether an element should be included
//
// Returns:
//   - []A: A new slice containing only the elements that satisfy the criteria
func Filter[A any](coll []A, criteria func(i A) bool) []A {
	out := []A{}
	for _, item := range coll {
		if criteria(item) {
			out = append(out, item)
		}
	}
	return out
}

// Find returns the first element in a slice that satisfies the provided criteria function.
// If no element satisfies the criteria, nil is returned.
//
// Type Parameters:
//   - A: The type of elements in the slice
//
// Parameters:
//   - coll: The input slice to search
//   - criteria: Function that determines whether an element matches
//
// Returns:
//   - *A: Pointer to the first matching element, or nil if no match is found
func Find[A any](coll []*A, criteria func(i *A) bool) *A {
	for _, item := range coll {
		if criteria(item) {
			return item
		}
	}
	return nil
}

// Reduce applies a function against an accumulator and each element in the slice
// to reduce it to a single value.
//
// Type Parameters:
//   - A: The type of elements in the input slice
//   - B: The type of the accumulated result
//
// Parameters:
//   - coll: The input slice to reduce
//   - processor: Function that combines the accumulator with each element
//   - initialState: The initial value of the accumulator
//
// Returns:
//   - B: The final accumulated value
func Reduce[A any, B any](coll []A, processor func(accum B, next A) B, initialState B) B {
	val := initialState
	for _, item := range coll {
		val = processor(val, item)
	}
	return val
}

// Flatten combines multiple slices into a single slice.
//
// Type Parameters:
//   - A: The type of elements in the slices
//
// Parameters:
//   - coll: A slice of slices to flatten
//
// Returns:
//   - []A: A new slice containing all elements from all input slices
func Flatten[A any](coll [][]A) []A {
	out := []A{}
	for _, arr := range coll {
		out = append(out, arr...)
	}
	return out
}

// ShortenHex shortens a hexadecimal string (like an Ethereum address or hash)
// by keeping the first 6 characters and last 4 characters, separated by "..".
//
// Parameters:
//   - publicKey: The hexadecimal string to shorten
//
// Returns:
//   - string: The shortened string (e.g., "0x1234..abcd")
func ShortenHex(publicKey string) string {
	return publicKey[0:6] + ".." + publicKey[len(publicKey)-4:]
}

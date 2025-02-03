package helpers

func MergeCommittedStates(dest, src map[string][]interface{}) map[string][]interface{} {
	for k, v := range src {
		if _, ok := dest[k]; !ok {
			dest[k] = make([]interface{}, 0)
		}
		dest[k] = append(dest[k], v...)
	}
	return dest
}

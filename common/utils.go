package common

// MapWithFilter applies a mapper function to each element of the input slice,
// and includes the result in the output slice only if the mapper function returns true.
func MapWithFilter[T any, U any](input []T, mapper func(T) (U, bool)) []U {
	output := make([]U, 0)
	for _, v := range input {
		o, ok := mapper(v)
		if ok {
			output = append(output, o)
		}
	}
	return output
}

package pg

import (
	"math"
	"strconv"
	"strings"
)

func ParsePlanString(p string) map[string]string {
	result := map[string]string{}
	fields := strings.Split(p, ",")
	for _, field := range fields {
		keyval := strings.SplitN(field, ":", 2)
		result[keyval[0]] = keyval[1]
	}
	return result
}
func ConvertHexToFloat64(val string) (float64, error) {
	n, err := strconv.ParseUint(val, 16, 64)
	if err != nil {
		return 0, err
	}
	n2 := uint64(n)
	return math.Float64frombits(n2), nil

}

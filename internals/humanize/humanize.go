package humanize

import "strconv"

const three = 3

func Comma(number int64) string {
	sign := ""
	if number < 0 {
		sign = "-"
		number = -number
	}

	str := strconv.FormatInt(number, 10)
	length := len(str)

	if length <= three {
		return sign + str
	}

	str, res := str[:length-three], str[length-three:length]
	length -= three

	for ; length > three; length -= three {
		res = str[length-three:length] + "," + res
		str = str[:length-three]
	}

	return sign + str + "," + res
}

const thousand = 1_000

// Unit returns a human-readable string of n with a unit.
// Only support positive integers, and the maximum unit is "g".
func Unit(number int) string {
	unit := ""
	units := []string{"k", "m", "g"}

	for i := 0; number >= thousand; number, i = number/thousand, i+1 {
		unit = units[i]
	}

	return strconv.Itoa(number) + unit
}

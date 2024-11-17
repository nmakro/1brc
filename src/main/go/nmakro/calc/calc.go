package calc

import "math"

func CumAverage(index int, prev, current uint32) uint32 {
	prevFloat := math.Float32frombits(prev)
	currFloat := math.Float32frombits(current)
	// Since we know these are scaled by 10, we can maintain precision
	// without generating additional decimal places
	result := (currFloat + float32(index)*prevFloat) / float32(index+1)
	return math.Float32bits(result)
}

func Min(index int, prev, current uint32) uint32 {
	prevFloat := math.Float32frombits(prev)
	currFloat := math.Float32frombits(current)
	if index > 0 {
		if currFloat < prevFloat {
			return current
		}
		return prev
	}
	return current
}

func Max(index int, prev, current uint32) uint32 {
	prevFloat := math.Float32frombits(prev)
	currFloat := math.Float32frombits(current)
	if index > 0 {
		if currFloat > prevFloat {
			return current
		}
		return prev
	}
	return current
}

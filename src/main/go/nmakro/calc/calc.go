package calc

func CumAverage(index int, prev, current float32) float32 {
	return (current + (float32(index))*prev) / float32(index+1)
}

func Min(index int, prev, current float32) float32 {
	if index > 0 {
		if current < prev {
			return current
		}
		return prev
	}
	return current
}

func Max(index int, prev, current float32) float32 {
	if index > 0 {
		if current > prev {
			return current
		}
		return prev
	}
	return current
}

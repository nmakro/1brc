package calc

func CumAverage(index int, prev, current float32) float32 {
	return (current + (float32(index))*prev) / float32(index+1)
}

func Min(prev, current float32) float32 {
	if current < prev {
		return current
	}
	return prev
}

func Max(prev, current float32) float32 {
	if current > prev {
		return current
	}
	return prev
}

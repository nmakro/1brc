package calc

import "testing"

func TestCumAverage(t *testing.T) {

	measurements := []float32{1, 2, 3, 1, 3}

	total_average := float32(2)

	average := float32(0)

	for i, m := range measurements {
		average = CumAverage(i, average, m)
	}

	if average != total_average {
		t.Errorf("got %v, wanted %v", average, total_average)
	}
}

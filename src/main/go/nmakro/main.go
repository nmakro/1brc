package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"sync"
	"time"

	"github.com/nmakro/1brc/calc"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

const linesPerChunk = 100000 // Adjust this value as needed

type Station struct {
	Name []byte
	Measurement
}

type Measurement struct {
	Min   float32
	Max   float32
	Mean  float32
	Index int
}

type StationMeasurements map[string]Measurement

var Stations = make(StationMeasurements)

func (s StationMeasurements) Sort() {
	keys := maps.Keys(s)
	sort.Strings(keys)
}

func processLines(lines []string) (StationMeasurements, error) {
	localStations := make(StationMeasurements)
	for _, line := range lines {
		splitted := bytes.Split([]byte(line), []byte(";"))
		name := string(splitted[0])
		measurementString := string(splitted[1])

		measurementAsFloat, err := strconv.ParseFloat(measurementString, 32)
		if err != nil {
			return nil, fmt.Errorf("error parsing measurement: %v", err)
		}

		measurement := float32(measurementAsFloat)

		prevMeasurement := localStations[name]
		localStations[name] = Measurement{
			Min:   calc.Min(prevMeasurement.Min, measurement),
			Max:   calc.Max(prevMeasurement.Max, measurement),
			Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, measurement),
			Index: prevMeasurement.Index + 1,
		}
	}
	return localStations, nil
}

func main() {
	now := time.Now()
	MEASUREMENTS_PATH, err := filepath.Abs("../../../../data/measurements.txt")
	if err != nil {
		log.Fatal(err)
	}

	f, err := os.Open(MEASUREMENTS_PATH)
	if err != nil {
		log.Fatal(err)
	}
	info, err := f.Stat()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Printf("File size: %d bytes\n", info.Size())
	defer f.Close()

	numWorkers := runtime.NumCPU()

	results := make(chan StationMeasurements, numWorkers)
	eg, ctx := errgroup.WithContext(context.Background())

	scanner := bufio.NewScanner(f)
	var mu sync.Mutex
	lineBuffer := make([]string, 0, linesPerChunk)

	for i := 0; i < numWorkers; i++ {
		eg.Go(func() error {
			for {
				select {
				case <-ctx.Done():
					return ctx.Err()
				default:
					mu.Lock()
					if len(lineBuffer) == 0 {
						for j := 0; j < linesPerChunk && scanner.Scan(); j++ {
							lineBuffer = append(lineBuffer, scanner.Text())
						}
						if len(lineBuffer) == 0 {
							mu.Unlock()
							return nil // No more lines to process
						}
					}
					chunk := make([]string, len(lineBuffer))
					copy(chunk, lineBuffer)
					lineBuffer = lineBuffer[:0]
					mu.Unlock()

					localStations, err := processLines(chunk)
					if err != nil {
						return err
					}
					results <- localStations
				}
			}
		})
	}

	go func() {
		eg.Wait()
		close(results)
	}()

	finalStations := make(StationMeasurements)
	for localStations := range results {
		for name, measurement := range localStations {
			prevMeasurement := finalStations[name]
			finalStations[name] = Measurement{
				Min:   calc.Min(prevMeasurement.Min, measurement.Min),
				Max:   calc.Max(prevMeasurement.Max, measurement.Max),
				Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, measurement.Mean),
				Index: prevMeasurement.Index + measurement.Index,
			}
		}
	}

	if err := eg.Wait(); err != nil {
		log.Fatalf("Error processing file: %v", err)
	}

	keys := maps.Keys(finalStations)
	slices.Sort(keys)
	for _, key := range keys {
		fmt.Printf("%s=%.2f/%.2f/%.2f \n", key, finalStations[key].Min, finalStations[key].Mean, finalStations[key].Max)
	}
	fmt.Printf("Time taken: %s\n", time.Since(now))
}

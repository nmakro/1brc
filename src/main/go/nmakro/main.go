package main

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"sync"
	"time"

	"github.com/nmakro/1brc/calc"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

type Station struct {
	Name []byte
	Measurement
}

type Measurement struct {
	Min   uint32
	Max   uint32
	Mean  uint32
	Index int
}

type StationMeasurements map[string]Measurement

func processLines(lines [][]byte) (StationMeasurements, error) {
	localStations := make(StationMeasurements)
	for _, line := range lines {
		splitted := bytes.Split([]byte(line), []byte(";"))
		name := string(splitted[0])
		//measurementString := string(splitted[1])

		floatBits := parseFloatFromBytes(splitted[1])

		// measurementAsFloat, err := strconv.ParseFloat(measurementString, 32)
		// if err != nil {
		// 	return nil, fmt.Errorf("error parsing measurement: %v", err)
		// }

		prevMeasurement := localStations[name]
		msm := Measurement{
			Min:   calc.Min(prevMeasurement.Index, prevMeasurement.Min, floatBits),
			Max:   calc.Max(prevMeasurement.Index, prevMeasurement.Max, floatBits),
			Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, floatBits),
			Index: prevMeasurement.Index + 1,
		}
		localStations[name] = msm
	}
	return localStations, nil
}

func skipUntilNewline(reader *bufio.Reader) error {
	// Skip bytes until we find a newline character
	for {
		b, err := reader.ReadByte()
		if err != nil {
			return err
		}
		if b == '\n' {
			return nil
		}
	}
}

func parseFloatFromBytes(input []byte) uint32 {
	// For "-99.4" we'll return -994 as integer representation
	var result int32 = 0
	isNegative := false

	for _, b := range input {
		switch b {
		case '-':
			isNegative = true
		case '.':
			continue
		default:
			result = result*10 + int32(b-'0')
		}
	}

	if isNegative {
		result = -result
	}
	return math.Float32bits(float32(result) / 10)
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

	numWorkers := runtime.NumCPU() * 20

	fileSize := info.Size()

	linesPerChunk := fileSize / int64(numWorkers)

	fmt.Printf("File size: %d bytes\n", info.Size())
	defer f.Close()

	results := make(chan StationMeasurements, 100000)
	eg, _ := errgroup.WithContext(context.Background())

	for i := 0; i < numWorkers; i++ {
		start := int64(i) * linesPerChunk
		end := start + linesPerChunk
		if i == numWorkers-1 {
			end = info.Size()
		}

		eg.Go(func() error {
			lineBuffer := make([][]byte, 0, linesPerChunk/10)
			// Create a section reader for this chunk
			sectionReader := io.NewSectionReader(f, start, end-start)
			reader := bufio.NewReader(sectionReader)
			// If not the first worker, find the start of the next complete line
			if start > 0 {
				if err := skipUntilNewline(reader); err != nil {
					return err
				}
			}

			// Read lines until we reach the end of our chunk
			for {
				line, err := reader.ReadBytes('\n')
				if err != nil {
					if err == io.EOF {
						// If we're not the last worker and we have a partial line
						if len(line) > 0 && end < info.Size() {
							// Create a new section reader that extends beyond our chunk
							extendedReader := io.NewSectionReader(f, end, 1024) // Read up to 1KB more
							extraReader := bufio.NewReader(extendedReader)

							// Read until we find a newline
							extraBytes, err := extraReader.ReadBytes('\n')
							if err != nil && err != io.EOF {
								return err
							}

							// Combine the partial line with the extra bytes
							lineBuffer = append(lineBuffer, slices.Concat(line, extraBytes[:len(extraBytes)-1]))
							// Process the final partial line before returning
							localStations, err := processLines(lineBuffer)
							if err != nil {
								return err
							}
							results <- localStations
						}
						return nil
					}
					return err
				}

				lineBuffer = append(lineBuffer, line[:len(line)-1])
				if int64(len(lineBuffer)) == linesPerChunk {
					localStations, err := processLines(lineBuffer)
					if err != nil {
						return err
					}
					results <- localStations
					lineBuffer = lineBuffer[:0]
				}
			}
		})
	}

	finalStations := make(StationMeasurements)
	w := sync.WaitGroup{}
	w.Add(1)
	go func() {
		defer w.Done()
		for localStations := range results {
			for name, measurement := range localStations {
				prevMeasurement := finalStations[name]
				finalStations[name] = Measurement{
					Min:   calc.Min(prevMeasurement.Index, prevMeasurement.Min, measurement.Min),
					Max:   calc.Max(prevMeasurement.Index, prevMeasurement.Max, measurement.Max),
					Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, measurement.Mean),
					Index: prevMeasurement.Index + measurement.Index,
				}
			}
		}
	}()

	if err := eg.Wait(); err != nil {
		close(results)
		log.Fatalf("Error processing file: %v", err)
	}
	close(results)

	w.Wait()

	keys := maps.Keys(finalStations)
	slices.Sort(keys)
	for _, key := range keys {
		fmt.Printf("%s=%.1f/%.1f/%.1f, occurences=%d\n", key, math.Float32frombits(finalStations[key].Min), math.Float32frombits(finalStations[key].Mean), math.Float32frombits(finalStations[key].Max), finalStations[key].Index)
	}

	fmt.Printf("Time taken: %s\n", time.Since(now))
}

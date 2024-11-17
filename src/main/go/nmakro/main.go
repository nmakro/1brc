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
	"strconv"
	"sync"
	"time"

	"github.com/nmakro/1brc/calc"
	"golang.org/x/exp/maps"
	"golang.org/x/sync/errgroup"
)

type Station struct {
	Name string
	Measurement
}

var availableMemory = getAvailableMemory()

//var lineCount atomic.Uint64

type Measurement struct {
	Min   uint32
	Max   uint32
	Mean  uint32
	Index int
}

type StationMeasurements map[string]Measurement

type Line struct {
	Data        []byte
	SplitterPos uint64
}

const splitter = ';'

func processLines(lines []Line, localStations StationMeasurements) error {
	for i := range lines {
		idx := lines[i].SplitterPos
		if idx == 0 {
			return nil
		}
		floatBits := parseFloatFromBytes(lines[i].Data[idx+1:])

		// measurementAsFloat, err := strconv.ParseFloat(measurementString, 32)
		// if err != nil {
		// 	return nil, fmt.Errorf("error parsing measurement: %v", err)
		// }

		prevMeasurement := localStations[string(lines[i].Data[:idx])]
		localStations[string(lines[i].Data[:idx])] = Measurement{
			Min:   calc.Min(prevMeasurement.Index, prevMeasurement.Min, floatBits),
			Max:   calc.Max(prevMeasurement.Index, prevMeasurement.Max, floatBits),
			Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, floatBits),
			Index: prevMeasurement.Index + 1,
		}
	}
	return nil
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

func getAvailableMemory() uint64 {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	// On Unix-like systems, you can also read from /proc/meminfo
	if info, err := os.ReadFile("/proc/meminfo"); err == nil {
		for _, line := range bytes.Split(info, []byte{'\n'}) {
			if bytes.HasPrefix(line, []byte("MemAvailable:")) {
				fields := bytes.Fields(line)
				if len(fields) == 3 {
					if available, err := strconv.ParseUint(string(fields[1]), 10, 64); err == nil {
						// Convert from KB to bytes
						return available * 1024
					}
				}
			}
		}
	}

	// Fallback: use total memory minus allocated
	return memStats.Sys - memStats.Alloc
}

func main() {
	// CPU profiling
	// cpuFile, err := os.Create("cpu.prof")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer cpuFile.Close()
	// if err := pprof.StartCPUProfile(cpuFile); err != nil {
	// 	log.Fatal(err)
	// }
	// defer pprof.StopCPUProfile()

	// Memory profiling
	// memFile, err := os.Create("mem.prof")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	// defer memFile.Close()

	now := time.Now()
	MEASUREMENTS_PATH, err := filepath.Abs("../../../../measurements.txt")
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

	numWorkers := runtime.NumCPU()

	fileSize := info.Size()

	bytesPerWorker := fileSize / int64(numWorkers)

	//var batchSize = (availableMemory / uint64(numWorkers))

	fmt.Printf("File size: %d bytes\n", info.Size())
	defer f.Close()

	results := make(chan Station, 200000)
	eg, _ := errgroup.WithContext(context.Background())

	for i := 0; i < numWorkers; i++ {
		start := int64(i) * bytesPerWorker
		end := start + bytesPerWorker
		if i == numWorkers-1 {
			end = info.Size()
		}

		eg.Go(func() error {
			ioReads := 0
			defer func() {
				fmt.Printf("worker %d IO reads: %d\n", i, ioReads)
			}()
			// Create a section reader for this chunk
			sectionReader := io.NewSectionReader(f, start, end-start)
			sectionReader.Size()
			reader := bufio.NewReaderSize(sectionReader, 5*10*64*1024)
			// If not the first worker, find the start of the next complete line
			if start > 0 {
				if err := skipUntilNewline(reader); err != nil {
					return err
				}
			}

			partialLine := Line{Data: make([]byte, 100)}
			localStations := make(StationMeasurements, 500)
			// Read lines until we reach the end of our chunk
			//var ioTimeRead time.Time
			var TotalIORead int64

			defer func() {
				fmt.Printf("Total IO read time: %ds\n", TotalIORead)
			}()
			//buffer := make([]byte, reader.Size()) // 32KB buffer

			newLineFound := false
			lineBuffer := make([]Line, reader.Size()/100)
			sectionReaderCounter := start
			for {

				// if i == 0 {
				// 	ioTimeRead = time.Now()
				// }
				// if ioReads == 1 {
				// 	fmt.Println()
				// }

				//clear(buffer)
				buffer := make([]byte, reader.Size()) // 32KB buffer

				n, err := reader.Read(buffer)

				if err == io.EOF {
					fmt.Println()
				}
				// if i == 0 {
				// 	TotalIORead = TotalIORead + time.Since(ioTimeRead).Nanoseconds()
				// }

				ioReads++
				if err != nil && err != io.EOF {
					return err
				}

				//data := buffer[:n]
				bufferPos := 0

				// Find and process complete lines
				var line Line

				lineCounter := 0
			inner:
				for {
					line.Data = line.Data[:0]
					line.SplitterPos = 0
					butesProcessed := 0
					steps := 0

					if n-bufferPos < 10 {
						fmt.Println()
					}
					for i := bufferPos; i < n; i++ {
						newLineFound = false
						butesProcessed++

						if buffer[i] == '\n' {
							newLineFound = true
							break
						}

						line.Data = append(line.Data, buffer[i])
						if buffer[i] == ';' {
							line.SplitterPos = uint64(steps)
						}
						steps++
					}

					bufferPos += butesProcessed

					lineCopy := make([]byte, len(line.Data))
					copy(lineCopy, line.Data)

					if newLineFound {
						if len(partialLine.Data) > 0 {
							//line = append(partialLine, line...)
							lineBuffer[lineCounter] = Line{
								Data:        append(partialLine.Data, lineCopy...),
								SplitterPos: max(partialLine.SplitterPos, line.SplitterPos),
							}

							clear(partialLine.Data)

							partialLine.SplitterPos = 0
						} else {
							l := Line{Data: lineCopy, SplitterPos: line.SplitterPos}
							lineBuffer[lineCounter] = l
						}
						lineCounter++
					} else {
						fmt.Println()
					}

					if lineCounter == cap(lineBuffer) || bufferPos == n {
						//now := time.Now()
						err := processLines(lineBuffer, localStations)
						lineCounter = 0
						if err != nil {
							return err
						}
						// t := time.Since(now).Nanoseconds()
						// fmt.Println(t)
						for name, v := range localStations {
						inner2:
							for {
								select {
								case results <- Station{Name: name, Measurement: v}:
									break inner2
								default:
									fmt.Println("waiting")
								}
							}
						}

						maps.Clear(localStations)
						for i := range lineBuffer {
							lineBuffer[i].Data = lineBuffer[i].Data[:0]
							lineBuffer[i].SplitterPos = 0
						}
					}
					steps = 0
					if !newLineFound {
						if bufferPos < n {
							for i := bufferPos; i < n; i++ {
								steps++
								if buffer[i] == ';' {
									partialLine.SplitterPos = uint64(i)
								}
								copy(partialLine.Data, []byte{buffer[bufferPos:n][i]})
								//partialLine.Data = append(partialLine.Data, buffer[bufferPos:n][i])
							}
							bufferPos += steps
							break inner
						} else {
							num := copy(partialLine.Data, line.Data)
							fmt.Println(num)
							break inner
						}
					}
				}
				// Save any remaining partial line for next buffer
				if bufferPos < n {
					steps := 0
					for i := bufferPos; i < n; i++ {
						if buffer[bufferPos:n][i] == ';' {
							partialLine.SplitterPos = uint64(steps)
						}
						copy(partialLine.Data, []byte{buffer[bufferPos:n][i]})
						//partialLine.Data = append(partialLine.Data, buffer[bufferPos:n][i])
						steps++
					}
				}

				if bufferPos == n || err == io.EOF {
					// Handle final partial line at chunk boundary
					if len(partialLine.Data) > 0 && end < info.Size() {

						line := Line{Data: make([]byte, 0, 150)}
						for i := range partialLine.Data {
							copy(line.Data, []byte{partialLine.Data[i]})
							if partialLine.Data[i] == ';' {
								line.SplitterPos = uint64(i)
							}
						}
						// Create a new section reader that extends beyond our chunk
						//extendedReader := io.NewSectionReader(sectionReader, sectionReaderCounter+int64(bufferPos), 100)
						extendedReader := io.NewSectionReader(f, sectionReaderCounter+int64(bufferPos)+1, 100)
						extraReader := bufio.NewReader(extendedReader)

						// Read until we find a newline

						for {
							b, err := extraReader.ReadByte()
							if err != nil {
								if err == io.EOF {
									break
								}
								return err
							}
							if b == '\n' {
								break
							}
							line.Data = append(line.Data, b)
							if b == ';' {
								line.SplitterPos = 0
							}
						}

						// extraBytes, err2 := extraReader.ReadBytes('\n')
						// if err != nil && err2 != io.EOF {
						// 	return err2
						// }

						line.Data = append(line.Data, extraBytes...)
						for i, b := range line.Data {
							if b == ';' {
								line.SplitterPos = uint64(i)
								break
							}
						}

						// Combine the partial line with the extra bytes (excluding the newline)
						//line := append(partialLine, extraBytes[:len(extraBytes)-1]...)
						lineBuffer[0] = line
						// Process any remaining lines
						if len(lineBuffer) > 0 {
							err3 := processLines(lineBuffer, localStations)
							if err3 != nil {
								return err3
							}

							for name, v := range localStations {
								results <- Station{Name: name, Measurement: v}
							}
						}
					}

					if err == io.EOF {
						return nil
					}
				}
			}
		})
	}

	finalStations := make(StationMeasurements, 500)
	w := sync.WaitGroup{}
	w.Add(1)
	waitTimes := 0
	routineRunTime := time.Now()
	go func() {
		defer w.Done()
		defer func() {
			fmt.Printf("Routine run time: %s\n", time.Since(routineRunTime))
		}()
		// for {
		// 	select {
		// 	case res, ok := <-results:
		// 		if !ok {
		// 			return
		// 		}
		// 		for name, measurement := range res {
		// 			prevMeasurement := finalStations[name]
		// 			finalStations[name] = Measurement{
		// 				Min:   calc.Min(prevMeasurement.Index, prevMeasurement.Min, measurement.Min),
		// 				Max:   calc.Max(prevMeasurement.Index, prevMeasurement.Max, measurement.Max),
		// 				Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, measurement.Mean),
		// 				Index: prevMeasurement.Index + measurement.Index,
		// 			}
		// 		}
		// 	default:
		// 		waitTimes++
		// 	}
		// }

		for localStations1 := range results {
			prevMeasurement := finalStations[localStations1.Name]
			finalStations[localStations1.Name] = Measurement{
				Min:   calc.Min(prevMeasurement.Index, prevMeasurement.Min, localStations1.Min),
				Max:   calc.Max(prevMeasurement.Index, prevMeasurement.Max, localStations1.Max),
				Mean:  calc.CumAverage(prevMeasurement.Index, prevMeasurement.Mean, localStations1.Mean),
				Index: prevMeasurement.Index + localStations1.Index,
			}
		}

	}()

	if err := eg.Wait(); err != nil {
		close(results)
		log.Fatalf("Error processing file: %v", err)
	}
	close(results)

	w.Wait()

	fmt.Printf("Wait times: %d\n", waitTimes)

	keys := maps.Keys(finalStations)
	slices.Sort(keys)
	// for _, key := range keys {
	// 	fmt.Printf("%s=%.1f/%.1f/%.1f, occurences=%d\n", key, math.Float32frombits(finalStations[key].Min), math.Float32frombits(finalStations[key].Mean), math.Float32frombits(finalStations[key].Max), finalStations[key].Index)
	// }

	fmt.Printf("Time taken: %s\n", time.Since(now))
	//fmt.Printf("Lines read: %d\n", lineCount.Load())

	// Write memory profile
	// runtime.GC()
	// if err := pprof.WriteHeapProfile(memFile); err != nil {
	// 	log.Fatal(err)
	// }
}

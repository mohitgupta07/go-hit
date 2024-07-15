package sfw

import (
	"fmt"
	"os"
	"strconv"
	"testing"
)

const benchmarkDir = "./benchmark_data"

// BenchmarkSaveToDisk benchmarks the SaveToDisk method
func BenchmarkSaveToDisk(b *testing.B) {
	// Create a new instance of SFWPersistence for benchmarking
	persistenceObj, err := NewSFWPersistence(benchmarkDir, 10) // Use a higher ioLimit for benchmarks
	persistence := persistenceObj.(*SFWPersistence)
	if err != nil {
		b.Fatalf("Error creating persistence object: %v", err)
	}
	defer cleanupBenchmarkData()

	// Reset benchmark timer before starting
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		op := "save"
		persistence.SaveToDisk(key, value, op)
	}

	// Wait for all operations to finish
	persistence.wg.Wait()
}

// BenchmarkLoad benchmarks the Load method
func BenchmarkLoad(b *testing.B) {
	// Create a new instance of SFWPersistence for benchmarking
	persistenceObj, err := NewSFWPersistence(benchmarkDir, 10) // Use a higher ioLimit for benchmarks
	persistence := persistenceObj.(*SFWPersistence)
	if err != nil {
		b.Fatalf("Error creating persistence object: %v", err)
	}
	defer cleanupBenchmarkData()

	// Populate benchmark data
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		op := "save"
		persistence.SaveToDisk(key, value, op)
	}
	persistence.wg.Wait()
	fmt.Println("saving done")
	// Reset benchmark timer before starting
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		_, err := persistence.Load()
		if err != nil {
			b.Fatalf("Error loading data: %v", err)
		}
	}
}

// cleanupBenchmarkData deletes the benchmark directory and its contents
func cleanupBenchmarkData() {
	os.RemoveAll(benchmarkDir)
}

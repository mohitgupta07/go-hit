package cassandra

import (
	"fmt"
	"strconv"
	"testing"
)

const cassandraKeyspace = "test_keyspace"
const cassandratable = "test_table"
const ioVal = 500

// BenchmarkSaveToDisk benchmarks the SaveToDisk method for Cassandra
func BenchmarkSaveToDisk(b *testing.B) {
	clusterHosts := []string{"127.0.0.1"}
	// Create a new instance of CassandraStore for benchmarking
	persistence, err := NewCassandraStore(clusterHosts, cassandraKeyspace, cassandratable, ioVal)
	if err != nil {
		b.Fatalf("Error creating persistence object: %v", err)
	}

	// Reset benchmark timer before starting
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		op := "set"
		persistence.SaveToDisk(key, value, op)
	}
	cleanupBenchmarkData(persistence)
	persistence.ShutDown()
}

// BenchmarkLoad benchmarks the Load method for Cassandra
func BenchmarkLoad(b *testing.B) {
	clusterHosts := []string{"127.0.0.1"}
	// Create a new instance of CassandraStore for benchmarking
	persistence, err := NewCassandraStore(clusterHosts, cassandraKeyspace, cassandratable, ioVal)
	if err != nil {
		b.Fatalf("Error creating persistence object: %v", err)
	}

	// Populate benchmark data
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		op := "set"
		persistence.SaveToDisk(key, value, op)
	}
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
	cleanupBenchmarkData(persistence)
	persistence.ShutDown()
	fmt.Println("Load testing done")
}

// cleanupBenchmarkData deletes the benchmark data from Cassandra
func cleanupBenchmarkData(s *CassandraStore) {
	s.dropCurrentTable()
}

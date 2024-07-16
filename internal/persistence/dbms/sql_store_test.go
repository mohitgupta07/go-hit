package dbms

import (
	"database/sql"
	"fmt"
	"log"
	"strconv"
	"testing"
)

const connKey = "postgres://newuser:password@localhost/postgres?sslmode=disable"
const ioVal = 10

// BenchmarkSaveToDisk benchmarks the SaveToDisk method
func BenchmarkSaveToDisk(b *testing.B) {
	// Create a new instance of SFWPersistence for benchmarking
	persistenceObj, err := NewSQLStore(connKey, ioVal) // Use a higher ioLimit for benchmarks
	persistence := persistenceObj.(*SQLStore)
	if err != nil {
		b.Fatalf("Error creating persistence object: %v", err)
	}
	defer cleanupBenchmarkData(persistence.db)

	// Reset benchmark timer before starting
	b.ResetTimer()

	// Run the benchmark
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		op := "save"
		persistence.SaveToDisk(key, value, op)
	}
}

// BenchmarkLoad benchmarks the Load method
func BenchmarkLoad(b *testing.B) {
	// Create a new instance of SFWPersistence for benchmarking
	persistenceObj, err := NewSQLStore(connKey, ioVal) // Use a higher ioLimit for benchmarks
	persistence := persistenceObj.(*SQLStore)
	if err != nil {
		b.Fatalf("Error creating persistence object: %v", err)
	}
	defer cleanupBenchmarkData(persistence.db)

	// Populate benchmark data
	for i := 0; i < b.N; i++ {
		key := "key" + strconv.Itoa(i)
		value := "value" + strconv.Itoa(i)
		op := "save"
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
}

// cleanupBenchmarkData deletes the benchmark directory and its contents
func cleanupBenchmarkData(db *sql.DB) {
	// Drop the table
	_, err := db.Exec("DROP TABLE IF EXISTS kv_store")
	if err != nil {
		log.Fatalln("Cannot drop table")
	}
}

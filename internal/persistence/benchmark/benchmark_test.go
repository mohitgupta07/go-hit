package benchmark

import (
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	// "os/signal"
	"sync"
	// "syscall"
	"testing"
)

var dataDir = "data"

// Function to initialize the data directory
func init() {
	err := os.MkdirAll(dataDir, 0755)
	if err != nil {
		fmt.Println("Error creating directory:", err)
		// Handle the error as needed, such as exiting the program or logging
	}
}

// Generate a random JSON value
func generateRandomJSON() []byte {
	data := map[string]interface{}{
		"name":    fmt.Sprintf("name-%d", rand.Intn(1000)),
		"age":     rand.Intn(100),
		"address": fmt.Sprintf("address-%d", rand.Intn(1000)),
	}
	jsonData, _ := json.Marshal(data)
	return jsonData
}

// Write a key-value pair to a file
func writeRandomKeyValuePair(key int, wg *sync.WaitGroup) {
	defer wg.Done()

	filename := fmt.Sprintf("%s/key-%d.json", dataDir, key)
	file, err := os.OpenFile(filename, os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		if os.IsNotExist(err) {
			// Handle case where file doesn't exist
			file, err = os.Create(filename) // Create new file if not exist
			if err != nil {
				panic(err)
			}
		} else {
			panic(err)
		}
	}
	defer file.Close()

	_, err = file.Write(generateRandomJSON())
	if err != nil {
		fmt.Println("Error writing to file:", err)
	}
}

func BenchmarkPerformance(b *testing.B) {
	// rand.Seed(time.Now().UnixNano())

	// Create a pool of workers
	const numWorkers = 40
	var wg sync.WaitGroup
	jobs := make(chan int, 30000)

	// Start worker goroutines
	for i := 0; i < numWorkers; i++ {
		go func() {
			for key := range jobs {
				writeRandomKeyValuePair(key, &wg)	
			}
		}()
	}

	// Run the benchmark
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		wg.Add(1)
		jobs <- i
	}
	close(jobs)
	wg.Wait()
}

// func setupCleanup() {
// 	// Register a shutdown hook to clean up the 'data' directory
// 	c := make(chan os.Signal, 1)
// 	signal.Notify(c, os.Interrupt, syscall.SIGTERM)
// 	go func() {
// 		<-c
// 		fmt.Println("\nCleaning up...")
// 		err := os.RemoveAll(dataDir)
// 		if err != nil {
// 			fmt.Println("Error removing directory:", err)
// 		}
// 		os.Exit(1)
// 	}()
// }

// func TestMain(m *testing.M) {
// 	// Setup cleanup handler
// 	setupCleanup()

// 	// Run tests
// 	exitCode := m.Run()

// 	// Cleanup (if needed)
// 	os.Exit(exitCode)
// }

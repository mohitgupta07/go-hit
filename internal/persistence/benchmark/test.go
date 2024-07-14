package benchmark

// import (
// 	"encoding/json"
// 	"fmt"
// 	"math/rand"
// 	"os"
// 	"sync"
// 	"testing"
// 	"time"
// )

// // Generate a random JSON value
// func generateRandomJSON() []byte {
// 	data := map[string]interface{}{
// 		"name":    fmt.Sprintf("name-%d", rand.Intn(1000)),
// 		"age":     rand.Intn(100),
// 		"address": fmt.Sprintf("address-%d", rand.Intn(1000)),
// 	}
// 	jsonData, _ := json.Marshal(data)
// 	return jsonData
// }

// // Write a key-value pair to a file
// func writeKeyValuePair(key int, wg *sync.WaitGroup) {
// 	defer wg.Done()
// 	filename := fmt.Sprintf("data/key-%d.json", key)
// 	file, err := os.Create(filename)
// 	if err != nil {
// 		fmt.Println("Error creating file:", err)
// 		return
// 	}
// 	defer file.Close()

// 	_, err = file.Write(generateRandomJSON())
// 	if err != nil {
// 		fmt.Println("Error writing to file:", err)
// 	}
// }

// func TestPerformance(t *testing.T) {
// 	rand.Seed(time.Now().UnixNano())

// 	// Create a pool of workers
// 	const numWorkers = 100
// 	var wg sync.WaitGroup
// 	jobs := make(chan int, 100000)

// 	// Start worker goroutines
// 	for i := 0; i < numWorkers; i++ {
// 		go func() {
// 			for key := range jobs {
// 				writeKeyValuePair(key, &wg)
// 			}
// 		}()
// 	}

// 	start := time.Now()

// 	// Dispatch jobs to workers
// 	for i := 0; i < 100000; i++ {
// 		wg.Add(1)
// 		jobs <- i
// 	}

// 	close(jobs)
// 	wg.Wait()

// 	elapsed := time.Since(start)
// 	fmt.Printf("Time taken: %s\n", elapsed)
// }

// func main() {
// 	// Ensure the test is run when executing main
// 	t := &testing.T{}
// 	TestPerformance(t)
// }

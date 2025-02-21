package sfw

// Fixed Number of Workers with a Job Queue
import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Function to load content from a single file
func singleLoad(filepath string) (string, string, error) {
	file, err := os.Open(filepath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	// reader := bufio.NewReader(file)
	decoder := json.NewDecoder(file)

	var data map[string]string
	if err := decoder.Decode(&data); err != nil {
		return "", "", err
	}

	if len(data) != 1 {
		return "", "", fmt.Errorf("expected single key-value pair in file")
	}

	for key, value := range data {
		return key, value, nil
	}

	return "", "", fmt.Errorf("no key-value pair found in file")
}

// Worker function that processes files and stores results locally
func worker(id int, file_jobs <-chan string, wg *sync.WaitGroup,
	localData []map[string]string) {
	defer wg.Done()
	threadLocalData := localData[id]

	for filepath := range file_jobs {
		key, value, err := singleLoad(filepath)
		if err == nil {
			threadLocalData[key] = value
		}
	}
}

// Function to merge results from all workers
func mergeResults(localData []map[string]string) map[string]string {
	finalData := make(map[string]string)
	for _, data := range localData {
		for key, value := range data {
			finalData[key] = value
		}
	}
	return finalData
}

// Parallel merge function to merge results from all workers concurrently
func parallelMerge(localData []map[string]string) map[string]string {
	finalData := make(map[string]string)
	var wg sync.WaitGroup
	var mu sync.Mutex

	// Number of goroutines should ideally be limited to prevent excessive parallelism
	// Adjust maxGoroutines based on the workload and system capabilities
	maxGoroutines := 5000
	chunkSize := (len(localData) + maxGoroutines - 1) / maxGoroutines // Ceiling division

	// Slice of worker IDs to process concurrently
	workerIDs := make([]int, 0, len(localData))
	for id := range localData {
		workerIDs = append(workerIDs, id)
	}

	for start := 0; start < len(workerIDs); start += chunkSize {
		end := start + chunkSize
		if end > len(workerIDs) {
			end = len(workerIDs)
		}

		wg.Add(1)
		go func(start, end int) {
			defer wg.Done()
			localFinalData := make(map[string]string)
			for _, id := range workerIDs[start:end] {
				for key, value := range localData[id] {
					localFinalData[key] = value
				}
			}
			// Merge localFinalData into finalData
			mu.Lock()
			for key, value := range localFinalData {
				finalData[key] = value
			}
			mu.Unlock()
		}(start, end)
	}

	wg.Wait()
	return finalData
}

// Loader utility function to manage workers and aggregate results
func LoaderUtil(dirpath string, numWorkers int) map[string]string {
	dir, err := os.Open(dirpath)
	if err != nil {
		fmt.Println("Error opening directory:", err)
		return nil
	}
	defer dir.Close()

	files, err := dir.Readdir(-1)
	if err != nil {
		fmt.Println("Error reading directory entries:", err)
		return nil
	}

	file_jobs := make(chan string, len(files))
	// localData := make(map[int]map[string]string)
	localData := make([]map[string]string, numWorkers)
	for i := range localData {
		localData[i] = make(map[string]string)
	}

	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go worker(i, file_jobs, &wg, localData)
	}

	for _, file := range files {
		filepath := filepath.Join(dirpath, file.Name())
		file_jobs <- filepath
	}

	// shutdown
	close(file_jobs)

	wg.Wait()

	// return localData[0]

	finalData := mergeResults(localData)

	return finalData
}

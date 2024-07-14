package persistence

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
	localData map[int]map[string]string, mu *sync.Mutex) {
	defer wg.Done()
	threadLocalData := make(map[string]string)

	for filepath := range file_jobs {
		key, value, err := singleLoad(filepath)
		if err == nil {
			threadLocalData[key] = value
		}
	}

	mu.Lock()
	localData[id] = threadLocalData
	mu.Unlock()
}

// Function to merge results from all workers
func mergeResults(localData map[int]map[string]string) map[string]string {
	finalData := make(map[string]string)
	for _, data := range localData {
		for key, value := range data {
			finalData[key] = value
		}
	}
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
	localData := make(map[int]map[string]string)
	var mu sync.Mutex
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	for i := 0; i < numWorkers; i++ {
		go worker(i, file_jobs, &wg, localData, &mu)
	}

	for _, file := range files {
		filepath := filepath.Join(dirpath, file.Name())
		file_jobs <- filepath
	}
	close(file_jobs)

	wg.Wait()

	// return localData[0]

	finalData := mergeResults(localData)

	return finalData
}

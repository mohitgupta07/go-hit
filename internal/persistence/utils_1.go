package persistence

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

func singleLoad(filepath string) (string, string, error) {
	// Open file
	file, err := os.Open(filepath)
	if err != nil {
		return "", "", err
	}
	defer file.Close()

	// Read content
	stat, err := file.Stat()
	if err != nil {
		return "", "", err
	}
	content := make([]byte, stat.Size())
	_, err = file.Read(content)
	if err != nil {
		return "", "", err
	}

	// Unmarshal JSON content into a map
	var data map[string]string
	err = json.Unmarshal(content, &data)
	if err != nil {
		return "", "", err
	}

	// Check if the map has a single key-value pair
	if len(data) != 1 {
		return "", "", fmt.Errorf("expected single key-value pair in file")
	}

	// Return the key and value
	for key, value := range data {
		return key, value, nil
	}

	return "", "", fmt.Errorf("no key-value pair found in file")
}

func worker(id int, file_jobs <-chan string, file_results chan<- map[string]string, wg *sync.WaitGroup) {
	defer wg.Done()
	localData := make(map[string]string)

	for filepath := range file_jobs {
		// Load data using singleLoad function and store in localData
		key, value, err := singleLoad(filepath)
		if err == nil { // Fix condition here
			localData[key] = value
		}
	}

	// Send local data map to file_results channel
	file_results <- localData
}

func mergefile_results(file_results <-chan map[string]string, done chan<- struct{}) map[string]string {
	finalData := make(map[string]string)

	for data := range file_results {
		for key, value := range data {
			finalData[key] = value
		}
	}

	done <- struct{}{} // Signal completion
	return finalData
}

func LoaderUtil(dirpath string, numWorkers int) map[string]string {
	// Open the directory
	dir, err := os.Open(dirpath)
	if err != nil {
		fmt.Println("Error opening directory:", err)
		return nil
	}
	defer dir.Close()

	// Read directory entries
	files, err := dir.Readdir(-1)
	if err != nil {
		fmt.Println("Error reading directory entries:", err)
		return nil
	}

	// Buffered channels for file_jobs and file_results
	file_jobs := make(chan string, len(files))
	file_results := make(chan map[string]string, len(files))
	done := make(chan struct{})

	// Worker pool
	var wg sync.WaitGroup
	wg.Add(numWorkers)

	// Launch workers
	for i := 0; i < numWorkers; i++ {
		go worker(i, file_jobs, file_results, &wg)
	}

	// Feed file_jobs to the workers
	for _, file := range files {
		filepath := filepath.Join(dirpath, file.Name())
		file_jobs <- filepath
	}
	close(file_jobs)

	// Launch a goroutine to close file_results after all workers are done
	go func() {
		wg.Wait()
		close(file_results)
	}()

	// Merge file_results concurrently
	var finalData map[string]string
	go func() {
		finalData = mergefile_results(file_results, done)
	}()

	// Wait for merging to complete
	<-done

	return finalData
}

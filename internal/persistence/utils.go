package persistence

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

type SafeMap struct {
	mu sync.Mutex
	m  map[string]string // Assuming value type as string for simplicity
}

func NewSafeMap() *SafeMap {
	return &SafeMap{
		m: make(map[string]string),
	}
}

func (sm *SafeMap) Set(key, value string) {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	sm.m[key] = value
}

func singleLoad(filepath string) (string, error) {
	// Open file
	file, err := os.Open(filepath)
	if err != nil {
		return "", err
	}
	defer file.Close()

	// Read content
	stat, err := file.Stat()
	if err != nil {
		return "", err
	}
	content := make([]byte, stat.Size())
	_, err = file.Read(content)
	if err != nil {
		return "", err
	}

	return string(content), nil
}

func updateMapWrapper(sm *SafeMap, filepath string) {
	data, err := singleLoad(filepath)
	if err != nil {
		fmt.Printf("Error loading file %s: %v\n", filepath, err)
		return
	}
	sm.Set(filepath, data)
	fmt.Printf("Updated SafeMap with file %s\n", filepath)
}

func processDirConcurrently(dirpath string, K int) *SafeMap {
	resultMap := make(map[string]string)
	var wg sync.WaitGroup
	semaphore := make(chan struct{}, K) // Semaphore to limit concurrent goroutines
	mu := sync.Mutex{}                  // Mutex for resultMap

	files, err := os.ReadDir(dirpath)
	if err != nil {
		fmt.Printf("Error reading directory %s: %v\n", dirpath, err)
		return nil
	}

	for _, file := range files {
		if file.IsDir() {
			continue // Skip directories
		}

		filepath := filepath.Join(dirpath, file.Name())

		wg.Add(1)
		semaphore <- struct{}{} // Acquire semaphore slot
		go func(fp string) {
			defer wg.Done()
			defer func() { <-semaphore }() // Release semaphore slot when done

			data, err := singleLoad(fp)
			if err != nil {
				fmt.Printf("Error loading file %s: %v\n", fp, err)
				return
			}

			mu.Lock()
			resultMap[fp] = data
			mu.Unlock()
			fmt.Printf("Processed file %s\n", fp)
		}(filepath)
	}

	wg.Wait()

	finalSafeMap := NewSafeMap()
	for key, value := range resultMap {
		finalSafeMap.Set(key, value)
	}

	return finalSafeMap
}

func LoadUtil(dirPath string, K int) map[string]string {
	sm := processDirConcurrently(dirPath, K)
	return sm.m
}

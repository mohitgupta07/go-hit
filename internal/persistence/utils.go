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

func startWorker(id int, filepath string, localData map[int]map[string]string, wg *sync.WaitGroup, semaphore chan<- struct{}) {
	defer wg.Done()
	semaphore <- struct{}{} // Release semaphore slot when done

	// Load data using singleLoad function and store in localData
	k, v, err := singleLoad(filepath)
	if err != nil {
		fmt.Printf("Error loading file %s: %v\n", filepath, err)
		return
	}
	localData[id][k] = v
}

func processDirConcurrently(dirPath string, K int) *SafeMap {
	var wg sync.WaitGroup
	finalSafeMap := NewSafeMap()

	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("Error reading directory %s: %v\n", dirPath, err)
		return nil
	}

	localMaps := make(map[int]map[string]string)
	for i := 0; i < K; i++ {
		localMaps[i] = make(map[string]string)
	}

	semaphore := make(chan struct{}, K) // Semaphore to limit concurrent goroutines

	for id, file := range files {
		fmt.Println("ok", id%K, file)
		if file.IsDir() {
			continue // Skip directories
		}

		filepath := filepath.Join(dirPath, file.Name())

		wg.Add(1)
		// Acquire semaphore slot
		go startWorker(id%K, filepath, localMaps, &wg, semaphore)
		<-semaphore
	}

	wg.Wait()

	// Combine all local maps into finalSafeMap
	finalSafeMap.mu.Lock()
	defer finalSafeMap.mu.Unlock()
	for _, localMap := range localMaps {
		for key, value := range localMap {
			finalSafeMap.m[key] = value
		}
	}

	return finalSafeMap
}

func LoadUtil(dirPath string, K int) map[string]string {
	sm := processDirConcurrently(dirPath, K)
	return sm.m
}

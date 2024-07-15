package sfw

// One Worker and one data per Job. See how combinedData is working here.
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

func startWorker(id int, filepath string, localData *SafeMap, wg *sync.WaitGroup) {
	defer wg.Done()

	// Load data using singleLoad function and store in localData
	k, v, err := singleLoad(filepath)
	if err != nil {
		fmt.Printf("Error loading file %s: %v\n", filepath, err)
		return
	}
	// localData.Set(k, v)
	localData.m[k] = v
}

func processDirConcurrently(dirPath string, K int) *SafeMap {
	var wg sync.WaitGroup
	finalSafeMap := NewSafeMap()

	files, err := os.ReadDir(dirPath)
	if err != nil {
		fmt.Printf("Error reading directory %s: %v\n", dirPath, err)
		return nil
	}

	combinedData := make([]*SafeMap, len(files))
	for i := 0; i < len(files); i++ {
		combinedData[i] = NewSafeMap()
	}

	semaphore := make(chan struct{}, K) // Semaphore to limit concurrent goroutines

	for id, file := range files {
		// fmt.Println("ok", id%K, file)
		if file.IsDir() {
			continue // Skip directories
		}

		filepath := filepath.Join(dirPath, file.Name())

		wg.Add(1)
		go func(id int) {
			semaphore <- struct{}{} // Acquire a token
			startWorker(id, filepath, combinedData[id], &wg)
			<-semaphore // Release the token
		}(id)
	}

	wg.Wait()

	// Combine all local maps into finalSafeMap
	finalSafeMap.mu.Lock()
	defer finalSafeMap.mu.Unlock()
	for _, localMap := range combinedData {
		for key, value := range localMap.m {
			finalSafeMap.m[key] = value
		}
	}

	return finalSafeMap
}

func LoadUtil(dirPath string, K int) map[string]string {
	sm := processDirConcurrently(dirPath, K)
	return sm.m
}

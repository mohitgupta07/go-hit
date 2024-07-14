// internal/datastore/datastore.go

package datastore

import (
	"log"
	"sync"
	"time"

	"github.com/Mohitgupta07/go-hit/internal/persistence" // Import persistence package
)

type KeyValueStore struct {
	store       map[string]string
	mu          sync.RWMutex
	saveQueue   chan saveRequest
	quitWorker  chan struct{}
	persistence persistence.Persistence
}

type saveRequest struct {
	Key   string
	Value string
	Op    string // "set" or "delete"
}

func NewKeyValueStore(p persistence.Persistence) *KeyValueStore {
	kv := &KeyValueStore{
		store:       make(map[string]string),
		saveQueue:   make(chan saveRequest, 1000), // Buffer channel to handle requests
		quitWorker:  make(chan struct{}),
		persistence: p,
	}
	go kv.worker() // Start the worker goroutine
	kv.loadFromPersistence()
	return kv
}

func (kv *KeyValueStore) Set(key, value string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.store[key] = value
	kv.enqueueSaveRequest(key, value, "set")
}

func (kv *KeyValueStore) Get(key string) string {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	return kv.store[key]
}

func (kv *KeyValueStore) Delete(key string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	delete(kv.store, key)
	kv.enqueueSaveRequest(key, "", "delete")
}

func (kv *KeyValueStore) Exists(key string) bool {
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	_, exists := kv.store[key]
	return exists
}

func (kv *KeyValueStore) Close() {
	close(kv.quitWorker)
}

func (kv *KeyValueStore) enqueueSaveRequest(key, value, op string) {
	req := saveRequest{
		Key:   key,
		Value: value,
		Op:    op,
	}
	kv.saveQueue <- req
}

func (kv *KeyValueStore) loadFromPersistence() {
	data, err := kv.persistence.Load()
	if err != nil {
		// Handle error
		log.Println("Error loading data from persistence")
		return
	}
	kv.store = data
	log.Printf("Loaded data from persistence: %v\n", kv.store)
}

func (kv *KeyValueStore) worker() {
	ticker := time.NewTicker(5 * time.Second) // Example: Save to disk every 5 seconds
	defer ticker.Stop()

	for {
		select {
		case req := <-kv.saveQueue:
			log.Println("kv queue:", req)
			kv.persistence.SaveToDisk(req.Key, req.Value, req.Op) // Call persistence function from persistence package
			// case <-ticker.C:
			// 	kv.persistence.SaveAllToDisk(kv.store) // Call persistence function from persistence package
			// case <-kv.quitWorker:
			// 	kv.persistence.SaveAllToDisk(kv.store) // Call persistence function from persistence package
			// 	return
		}
	}
}

func (kv *KeyValueStore) ShutDown() {
	kv.persistence.ShutDown()
}

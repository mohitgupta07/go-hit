package persistence

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"
)

type SFWPersistence struct {
	dirPath string
	queue   chan operation  // Channel for queuing operations
	wg      *sync.WaitGroup // WaitGroup to manage concurrent operations
	ioLimit int             // Maximum number of concurrent IO operations
}

type operation struct {
	key, value, op string
}

func NewSFWPersistence(dirPath string, ioLimit int) (*SFWPersistence, error) {
	err := os.MkdirAll(dirPath, 0755) // Ensure directory exists or create it
	if err != nil {
		return nil, fmt.Errorf("error creating directory: %v", err)
	}

	jp := &SFWPersistence{
		dirPath: dirPath,
		queue:   make(chan operation, ioLimit), // Buffered channel for queuing operations
		ioLimit: ioLimit,
		wg:      &sync.WaitGroup{},
	}

	// Start the write group in a separate goroutines.
	for i := 0; i < ioLimit; i++ {
		go jp.startWriteGroup()
	}

	return jp, nil
}

func (jp *SFWPersistence) SaveToDisk(key, value, op string) {
	// fmt.Println("SaveToDisk called:", key, value, op)
	jp.wg.Add(1)                          // Increment WaitGroup for new operation
	jp.queue <- operation{key, value, op} // Enqueue operation
}

func (jp *SFWPersistence) SaveAllToDisk(store map[string]string) {
	for key, value := range store {
		jp.SaveToDisk(key, value, "save") // Assuming "save" as the operation type
	}
}

func (jp *SFWPersistence) startWriteGroup() {
	for op := range jp.queue {
		// fmt.Println("called:", op)
		jp.writeData(op) // Start new goroutine to handle operation
	}
	fmt.Println("operation processed")
}

func (jp *SFWPersistence) writeData(op operation) {
	defer jp.wg.Done()

	data := map[string]string{op.key: op.value}
	if op.op == "delete" {
		data = map[string]string{op.key: ""}
	}

	filename := fmt.Sprintf("%s/key-%s.json", jp.dirPath, op.key)
	file, err := os.OpenFile(filename, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	err = enc.Encode(data)
	if err != nil {
		fmt.Println("Error encoding data to JSON:", err)
		return
	}
}

func (jp *SFWPersistence) ShutDown() {
	jp.CloseQueue()
	jp.Wait()
	fmt.Println("All operations processed")
}

func (jp *SFWPersistence) Wait() {
	jp.wg.Wait() // Wait for all operations to finish
}

func (jp *SFWPersistence) CloseQueue() {
	log.Println("len:", len(jp.queue))
	close(jp.queue) // Close the queue to stop the StartWriteGroup loop
}

func (jp *SFWPersistence) Load() (map[string]string, error) {
	return LoaderUtil(jp.dirPath, jp.ioLimit), nil
}

// Example of usage:
func ExampleUsage() {
	dirPath := "datastore"
	persistence, err := NewSFWPersistence(dirPath, 3) // Allow maximum 3 concurrent IO operations
	if err != nil {
		log.Fatal("Error creating persistence object:", err)
	}

	// Start the write group to consume queued operations
	go persistence.startWriteGroup()

	// Example operations (you can replace these with your actual usage scenarios)
	persistence.SaveToDisk("key1", "value1", "save")
	persistence.SaveToDisk("key2", "value2", "delete")
	persistence.SaveToDisk("key3", "value3", "save")

	// Wait for all operations to finish
	persistence.wg.Wait()

	// Example of loading all data from directory
	data, err := persistence.Load()
	if err != nil {
		log.Println("Error loading data:", err)
	} else {
		fmt.Println("Loaded data:", data)
	}
}

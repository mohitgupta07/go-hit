package sfw

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
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

// Ensure SFWPersistence implements the Persistence interface
var _ persistence.Persistence = (*SFWPersistence)(nil)

func NewSFWPersistence(dirPath string, ioLimit int) (persistence.Persistence, error) {
	err := os.MkdirAll(dirPath, 0755) // Ensure directory exists or create it
	if err != nil {
		return nil, fmt.Errorf("error creating directory: %v", err)
	}

	jp := &SFWPersistence{
		dirPath: dirPath,
		queue:   make(chan operation, 100000), // Buffered channel for queuing operations
		ioLimit: ioLimit,
		wg:      &sync.WaitGroup{},
	}

	// Start the write group in a separate goroutines.
	for i := 0; i < ioLimit; i++ {
		jp.wg.Add(1) // Increment WaitGroup for new operation
		go jp.startWriteGroup()
	}

	return jp, nil
}

func (jp *SFWPersistence) SaveToDisk(key, value, op string) {
	// fmt.Println("SaveToDisk called:", key, value, op)
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
	defer jp.wg.Done()
	// jp.wg.Done()
	// fmt.Println("operation processed")
}

func (jp *SFWPersistence) writeData(op operation) {

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
	// close the queue
	jp.CloseQueue()
	// once the queue is empty, go-routine workers get to know about it and wait for them to quit as well.
	jp.Wait()
	fmt.Println("All operations processed")
}

func (jp *SFWPersistence) Wait() {
	jp.wg.Wait() // Wait for all operations to finish
}

func (jp *SFWPersistence) CloseQueue() {
	// startTime := time.Now() // Start time measurement
	log.Println("queue process left", len(jp.queue))
	for len(jp.queue) > 0 {
	}

	close(jp.queue) // Close the queue to stop the StartWriteGroup loop

	// elapsedTime := time.Since(startTime) // Calculate elapsed time
	// log.Printf("Time taken to empty the queue with:  %s\n", elapsedTime)
}

func (jp *SFWPersistence) Load() (map[string]string, error) {
	return LoaderUtil(jp.dirPath, jp.ioLimit), nil
}

// Example of usage:
func ExampleUsage() {
	var per persistence.Persistence
	dirPath := "datastore"
	per, err := NewSFWPersistence(dirPath, 3) // Allow maximum 3 concurrent IO operations
	if err != nil {
		log.Fatal("Error creating persistence object:", err)
	}
	per2 := per.(*SFWPersistence)
	// Start the write group to consume queued operations
	go per2.startWriteGroup()

	// Example operations (you can replace these with your actual usage scenarios)
	per.SaveToDisk("key1", "value1", "save")
	per.SaveToDisk("key2", "value2", "delete")
	per.SaveToDisk("key3", "value3", "save")

	// Wait for all operations to finish
	per2.wg.Wait()

	// Example of loading all data from directory
	data, err := per.Load()
	if err != nil {
		log.Println("Error loading data:", err)
	} else {
		fmt.Println("Loaded data:", data)
	}
}

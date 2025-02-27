package sfw

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
)

type SFWPersistence struct {
	dirPath string
	queues  []chan *operation // Slice of channels for queuing operations
	wg      *sync.WaitGroup   // WaitGroup to manage concurrent operations
	ioLimit int               // Maximum number of concurrent IO operations
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
		queues:  make([]chan *operation, ioLimit), // Create multiple channels for queuing operations
		ioLimit: ioLimit,
		wg:      &sync.WaitGroup{},
	}

	// Initialize each queue channel and start the write group in separate goroutines.
	for i := 0; i < ioLimit; i++ {
		jp.queues[i] = make(chan *operation, 1000) // Buffered channel for queuing operations
		jp.wg.Add(1)                               // Increment WaitGroup for new operation
		go jp.startWriteGroup(i)
	}

	return jp, nil
}

func (jp *SFWPersistence) SaveToDisk(key, value, op string) {
	// todo: avoid blocking thread if one queue is full.
	// Randomly select a queue channel to push the operation
	rand.Seed(time.Now().UnixNano())
	randomQueue := jp.queues[rand.Intn(jp.ioLimit)]
	randomQueue <- &operation{key, value, op} // Enqueue operation
}

func (jp *SFWPersistence) SaveAllToDisk(store map[string]string) {
	for key, value := range store {
		jp.SaveToDisk(key, value, "save") // Assuming "save" as the operation type
	}
}

func (jp *SFWPersistence) startWriteGroup(queueIndex int) {
	defer jp.wg.Done()
	batchSize := 200 // Number of operations to batch together
	ops := make([]*operation, 0, batchSize)
	timer := time.NewTimer(10 * time.Millisecond)

	for {
		select {
		case op, ok := <-jp.queues[queueIndex]:
			if !ok {
				// Channel closed, process remaining operations
				jp.writeBatch(ops)
				return
			}
			ops = append(ops, op)
			if len(ops) >= batchSize {
				jp.writeBatch(ops)
				ops = ops[:0] // Reset the slice for the next batch
			}
		case <-timer.C:
			if len(ops) > 0 {
				jp.writeBatch(ops)
				ops = ops[:0] // Reset the slice for the next batch
			}
			timer.Reset(10 * time.Millisecond)
		}
	}
}

func (jp *SFWPersistence) writeBatch(ops []*operation) {
	for _, op := range ops {
		data := map[string]string{op.key: op.value}
		if op.op == "delete" {
			data = map[string]string{op.key: ""}
		}

		filePath := fmt.Sprintf("%s/%s.json", jp.dirPath, op.key)
		file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
		if err != nil {
			fmt.Println("Error opening file:", err)
			return
		}

		writer := bufio.NewWriter(file)
		enc := json.NewEncoder(writer)
		err = enc.Encode(data)
		if err != nil {
			fmt.Println("Error encoding data to JSON:", err)
			return
		}

		writer.Flush()
		file.Close()
	}
}

func (jp *SFWPersistence) ShutDown() {
	// close the queues
	jp.CloseQueues()
	// wait for all operations to quit as well.
	jp.Wait()
	fmt.Println("All operations processed")
}

func (jp *SFWPersistence) Wait() {
	jp.wg.Wait() // Wait for all operations to finish
}

func (jp *SFWPersistence) CloseQueues() {
	log.Println("Closing all queues")
	for _, queue := range jp.queues {
		close(queue)
	}
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

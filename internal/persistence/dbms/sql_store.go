package dbms

import (
	"database/sql"
	"log"
	"sync"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
	_ "github.com/lib/pq"
)

const createTableSQL = `
CREATE TABLE IF NOT EXISTS kv_store (
    key VARCHAR(255) PRIMARY KEY,
    value TEXT
)
`

// SQLStore represents a store backed by an SQL database.
type SQLStore struct {
	db      *sql.DB
	table   string
	writeCh chan operation  // Buffered channel for queuing write operations
	wg      *sync.WaitGroup // WaitGroup to manage concurrent operations
}

type operation struct {
	key, value, op string
}

// Function to create the kv_store table if it doesn't exist
func createKVStoreTable(db *sql.DB) error {
	_, err := db.Exec(createTableSQL)
	return err
}

// NewSQLStore initializes a new SQLStore.
func NewSQLStore(connStr string, maxConcurrency int) (persistence.Persistence, error) {
	table := "kv_store"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}
	// Ensure the table exists
	if err := createKVStoreTable(db); err != nil {
		log.Fatal(err)
	}
	log.Println("SQL Store:: PostgreSQL ready.")

	// Initialize SQLStore with concurrent write support
	store := &SQLStore{
		db:      db,
		table:   table,
		writeCh: make(chan operation, maxConcurrency), // Buffered channel for queuing operations
		wg:      &sync.WaitGroup{},
	}

	// Start worker goroutines to handle write operations
	for i := 0; i < maxConcurrency; i++ {
		go store.startWriteWorker()
	}

	return store, nil
}

// SaveToDisk writes a single key-value pair to the SQL database.
func (s *SQLStore) SaveToDisk(key, value, op string) {
	s.wg.Add(1)                            // Increment WaitGroup for new operation
	s.writeCh <- operation{key, value, op} // Enqueue operation
}

// SaveAllToDisk writes all key-value pairs to the SQL database concurrently.
func (s *SQLStore) SaveAllToDisk(store map[string]string) {
	for key, value := range store {
		s.SaveToDisk(key, value, "save") // Assuming "save" as the operation type
	}
}

// Load retrieves all key-value pairs from the SQL database.
func (s *SQLStore) Load() (map[string]string, error) {
	rows, err := s.db.Query("SELECT key, value FROM kv_store")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	store := make(map[string]string)
	for rows.Next() {
		var key, value string
		if err := rows.Scan(&key, &value); err != nil {
			return nil, err
		}
		store[key] = value
	}

	return store, nil
}

// ShutDown closes the database connection.
func (s *SQLStore) ShutDown() {
	close(s.writeCh) // Close the write channel to stop worker goroutines
	s.wg.Wait()      // Wait for all operations to finish
	err := s.db.Close()
	if err != nil {
		log.Printf("Error shutting down database: %v", err)
	}
}

// startWriteWorker starts a worker goroutine to handle write operations from writeCh.
func (s *SQLStore) startWriteWorker() {
	for op := range s.writeCh {
		s.writeData(op) // Process the operation
		s.wg.Done()     // Decrement WaitGroup when operation is complete
	}
}

// writeData performs the actual database write operation.
func (s *SQLStore) writeData(op operation) {
	if op.op == "delete" {
		_, err := s.db.Exec("DELETE FROM kv_store WHERE key = $1", op.key)
		if err != nil {
			log.Printf("Error deleting from disk: %v", err)
		}
	} else {
		_, err := s.db.Exec("INSERT INTO kv_store (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", op.key, op.value)
		if err != nil {
			log.Printf("Error saving to disk: %v", err)
		}
	}
}

// Ensure SQLStore implements persistence.Persistence interface
var _ persistence.Persistence = (*SQLStore)(nil)

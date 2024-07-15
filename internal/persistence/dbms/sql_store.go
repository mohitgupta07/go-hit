package dbms

import (
    "database/sql"
    "log"
    _ "github.com/lib/pq"
    "github.com/Mohitgupta07/go-hit/internal/persistence"
)

// SQLStore represents a store backed by an SQL database.
type SQLStore struct {
    db *sql.DB
}

// NewSQLStore initializes a new SQLStore.
func NewSQLStore(connStr string) (persistence.Persistence, error) {
    db, err := sql.Open("postgres", connStr)
    if err != nil {
        return nil, err
    }
    return &SQLStore{db: db}, nil
}

// SaveToDisk writes a single key-value pair to the SQL database.
func (s *SQLStore) SaveToDisk(key, value, op string) {
    if op == "delete" {
        _, err := s.db.Exec("DELETE FROM kv_store WHERE key = $1", key)
        if err != nil {
            log.Printf("Error deleting from disk: %v", err)
        }
    } else {
        _, err := s.db.Exec("INSERT INTO kv_store (key, value) VALUES ($1, $2) ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value", key, value)
        if err != nil {
            log.Printf("Error saving to disk: %v", err)
        }
    }
}

// SaveAllToDisk writes all key-value pairs to the SQL database.
func (s *SQLStore) SaveAllToDisk(store map[string]string) {
    for key, value := range store {
        s.SaveToDisk(key, value, "save")
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
    err := s.db.Close()
    if err != nil {
        log.Printf("Error shutting down database: %v", err)
    }
}

// Ensure SQLStore implements persistence.Persistence interface
var _ persistence.Persistence = (*SQLStore)(nil)

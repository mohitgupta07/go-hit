package dbms

import (
    "log"
    "github.com/gocql/gocql"
)

// CassandraStore represents a store backed by a Cassandra database.
type CassandraStore struct {
    session *gocql.Session
}

// NewCassandraStore initializes a new CassandraStore.
func NewCassandraStore(clusterHosts []string, keyspace string) (*CassandraStore, error) {
    cluster := gocql.NewCluster(clusterHosts...)
    cluster.Keyspace = keyspace
    session, err := cluster.CreateSession()
    if err != nil {
        return nil, err
    }
    return &CassandraStore{session: session}, nil
}

// SaveToDisk writes a single key-value pair to the Cassandra database.
func (s *CassandraStore) SaveToDisk(key, value, op string) {
    if op == "delete" {
        err := s.session.Query("DELETE FROM kv_store WHERE key = ?", key).Exec()
        if err != nil {
            log.Printf("Error deleting from disk: %v", err)
        }
    } else {
        err := s.session.Query("INSERT INTO kv_store (key, value) VALUES (?, ?)", key, value).Exec()
        if err != nil {
            log.Printf("Error saving to disk: %v", err)
        }
    }
}

// SaveAllToDisk writes all key-value pairs to the Cassandra database.
func (s *CassandraStore) SaveAllToDisk(store map[string]string) {
    for key, value := range store {
        s.SaveToDisk(key, value, "save")
    }
}

// Load retrieves all key-value pairs from the Cassandra database.
func (s *CassandraStore) Load() (map[string]string, error) {
    iter := s.session.Query("SELECT key, value FROM kv_store").Iter()
    store := make(map[string]string)
    var key, value string
    for iter.Scan(&key, &value) {
        store[key] = value
    }
    if err := iter.Close(); err != nil {
        return nil, err
    }

    return store, nil
}

// ShutDown closes the database connection.
func (s *CassandraStore) ShutDown() {
    s.session.Close()
}

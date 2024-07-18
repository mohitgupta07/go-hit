package cassandra

import (
	"fmt"
	"log"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
)

// // CassandraStore represents a store backed by a Cassandra database.
// type CassandraStore struct {
//     session *gocql.Session
//     writeCh chan operation
//     wg      sync.WaitGroup
// }

// operation represents a database operation.
type operation struct {
	key   string
	value string
	op    string
}

// // NewCassandraStore initializes a new CassandraStore.
// func NewCassandraStore(clusterHosts []string, keyspace string, numWorkers int) (persistence.Persistence, error) {
//     cluster := gocql.NewCluster(clusterHosts...)
//     cluster.Keyspace = keyspace
//     session, err := cluster.CreateSession()
//     if err != nil {
//         return nil, err
//     }

//     store := &CassandraStore{
//         session: session,
//         writeCh: make(chan operation),
//     }

//     for i := 0; i < numWorkers; i++ {
//         go store.startWriteWorker()
//     }

//     return store, nil
// }

// SaveToDisk writes a single key-value pair to the Cassandra database.
func (s *CassandraStore) SaveToDisk(key, value, op string) {
	// s.wg.Add(1)
	s.writeCh <- operation{key: key, value: value, op: op}
}

// SaveAllToDisk writes all key-value pairs to the Cassandra database.
func (s *CassandraStore) SaveAllToDisk(store map[string]string) {
	for key, value := range store {
		s.SaveToDisk(key, value, "save")
	}
	// s.wg.Wait()
}

// Load retrieves all key-value pairs from the Cassandra database.
func (s *CassandraStore) Load() (map[string]string, error) {
	iter := s.session.Query(fmt.Sprintf("SELECT key, value FROM %s.%s", s.keyspace, s.tablename)).Iter()
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
	close(s.writeCh)
	// s.wg.Wait()
	s.session.Close()
}

// startWriteWorker starts a worker goroutine to handle write operations from writeCh.
func (s *CassandraStore) startWriteWorker() {
	for op := range s.writeCh {
		s.writeData(op)
		// s.wg.Done()
	}
}

// writeData performs the actual database write operation.
func (s *CassandraStore) writeData(op operation) {
	switch op.op {
	case "delete":
		s.delete(op.key)
	case "set":
		if err := s.insert(op.key, op.value); err != nil {
			log.Printf("Failed to insert data: %v", err)
		}
	case "update":
		if err := s.update(op.key, op.value); err != nil {
			log.Printf("Failed to update data: %v", err)
		}
	}
}

func (s *CassandraStore) delete(key string) error {
	err := s.session.Query("DELETE FROM kv_store WHERE key = ?", key).Exec()
	if err != nil {
		return fmt.Errorf("error deleting from disk: %v", err)
	}
	return nil
}

func (s *CassandraStore) insert(key, value string) error {
	query := fmt.Sprintf("INSERT INTO %s.%s (key, value) VALUES (?, ?)", s.keyspace, s.tablename)
	if err := s.session.Query(query, key, value).Exec(); err != nil {
		return fmt.Errorf("failed to execute insert query: %v", err)
	}
	return nil
}

func (s *CassandraStore) update(key, value string) error {
	query := fmt.Sprintf("UPDATE %s.%s SET value = ? WHERE key = ?", s.keyspace, s.tablename)
	if err := s.session.Query(query, value, key).Exec(); err != nil {
		return fmt.Errorf("failed to execute update query: %v", err)
	}
	return nil
}

// Ensure CassandraStore implements persistence.Persistence interface
var _ persistence.Persistence = (*CassandraStore)(nil)

package cassandra

import (
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
	if op.op == "delete" {
		err := s.session.Query("DELETE FROM kv_store WHERE key = ?", op.key).Exec()
		if err != nil {
			log.Printf("Error deleting from disk: %v", err)
		}
	} else {
		err := s.session.Query("INSERT INTO kv_store (key, value) VALUES (?, ?)", op.key, op.value).Exec()
		if err != nil {
			log.Printf("Error saving to disk: %v", err)
		}
	}
}

// Ensure CassandraStore implements persistence.Persistence interface
var _ persistence.Persistence = (*CassandraStore)(nil)

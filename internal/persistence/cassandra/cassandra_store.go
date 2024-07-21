package cassandra

import (
	"fmt"
	"log"
	"sync"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
	"github.com/gocql/gocql"
)

type operation struct {
	key   string
	value string
	op    string
}

type CassandraStore struct {
	session   *gocql.Session
	writeCh   chan operation
	keyspace  string
	tablename string
	wg        *sync.WaitGroup // WaitGroup to manage concurrent operations
}

func NewCassandraStore(clusterHosts []string, keyspace string, tableName string, numWorkers int) (persistence.Persistence, error) {
	cluster := gocql.NewCluster(clusterHosts...)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %v", err)
	}
	// defer session.Close()

	// Check if keyspace exists
	keyspaceExists := false
	query := fmt.Sprintf("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name = '%s'", keyspace)
	iter := session.Query(query).Iter()
	var name string
	for iter.Scan(&name) {
		if name == keyspace {
			keyspaceExists = true
			break
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to check keyspace existence: %v", err)
	}

	// Create keyspace if it doesn't exist
	if !keyspaceExists {
		createKeyspaceQuery := fmt.Sprintf(`
            CREATE KEYSPACE %s WITH REPLICATION = {
                'class': 'SimpleStrategy',
                'replication_factor': 1
            }`, keyspace)
		if err := session.Query(createKeyspaceQuery).Exec(); err != nil {
			return nil, fmt.Errorf("failed to create keyspace: %v", err)
		}
		log.Printf("Created keyspace: %s", keyspace)
	}

	// Check if table exists in keyspace
	tableExists := false
	query = fmt.Sprintf("SELECT table_name FROM system_schema.tables WHERE keyspace_name = '%s' AND table_name = '%s'", keyspace, tableName)
	iter = session.Query(query).Iter()
	var tableNameFromDB string
	for iter.Scan(&tableNameFromDB) {
		if tableNameFromDB == tableName {
			tableExists = true
			break
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("failed to check table existence: %v", err)
	}

	// Create table if it doesn't exist
	if !tableExists {
		createTableQuery := fmt.Sprintf(`
            CREATE TABLE %s.%s (
                key TEXT PRIMARY KEY,
                value TEXT
            )`, keyspace, tableName)
		if err := session.Query(createTableQuery).Exec(); err != nil {
			return nil, fmt.Errorf("failed to create table: %v", err)
		}
		log.Printf("Created table: %s.%s", keyspace, tableName)
	}

	// Initialize CassandraStore
	store := &CassandraStore{
		session:   session,
		writeCh:   make(chan operation), // Buffered channel for better performance
		keyspace:  keyspace,
		tablename: tableName,
		wg:        &sync.WaitGroup{},
	}

	for i := 0; i < numWorkers; i++ {
		store.wg.Add(1)
		go store.startWriteWorker()
	}

	return store, nil
}

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
	s.wg.Wait()
	s.session.Close()
}

// startWriteWorker starts a worker goroutine to handle write operations from writeCh.
func (s *CassandraStore) startWriteWorker() {
	for op := range s.writeCh {
		s.writeData(op)
	}
	s.wg.Done()
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

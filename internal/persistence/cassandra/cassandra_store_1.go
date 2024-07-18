package cassandra

import (
	"fmt"
	"log"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
	"github.com/gocql/gocql"
)

type CassandraStore struct {
	session   *gocql.Session
	writeCh   chan operation
	keyspace  string
	tablename string
}

func NewCassandraStore(clusterHosts []string, keyspace string, tableName string, numWorkers int) (persistence.Persistence, error) {
	cluster := gocql.NewCluster(clusterHosts...)
	cluster.Keyspace = keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, fmt.Errorf("failed to create session: %v", err)
	}
	defer session.Close()

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
		writeCh:   make(chan operation, numWorkers), // Buffered channel for better performance
		keyspace:  keyspace,
		tablename: tableName,
	}

	for i := 0; i < numWorkers; i++ {
		go store.startWriteWorker()
	}

	return store, nil
}

// Ensure CassandraStore implements persistence.Persistence interface
var _ persistence.Persistence = (*CassandraStore)(nil)

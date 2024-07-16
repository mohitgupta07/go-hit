package cassandra

import (
	"fmt"
	"log"

	"github.com/Mohitgupta07/go-hit/internal/persistence"
	"github.com/gocql/gocql"
)

type CassandraStore struct {
	session *gocql.Session
	writeCh chan operation
}

func NewCassandraStore(clusterHosts []string, keyspace string, numWorkers int) (persistence.Persistence, error) {
	cluster := gocql.NewCluster(clusterHosts...)
	cluster.Keyspace = "system" // Use the system keyspace to check and create the target keyspace
	session, err := cluster.CreateSession()
	if err != nil {
		return nil, err
	}
	defer session.Close()

	// Check if keyspace exists
	keyspaceExists := false
	query := fmt.Sprintf("SELECT keyspace_name FROM system_schema.keyspaces WHERE keyspace_name='%s'", keyspace)
	iter := session.Query(query).Iter()
	var name string
	for iter.Scan(&name) {
		if name == keyspace {
			keyspaceExists = true
			break
		}
	}
	if err := iter.Close(); err != nil {
		return nil, err
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

	// Create a new session for the created keyspace
	cluster.Keyspace = keyspace
	session, err = cluster.CreateSession()
	if err != nil {
		return nil, err
	}

	// Initialize CassandraStore
	store := &CassandraStore{
		session: session,
		writeCh: make(chan operation, numWorkers), // Buffered channel for better performance
	}

	for i := 0; i < numWorkers; i++ {
		go store.startWriteWorker()
	}

	return store, nil
}

// Ensure CassandraStore implements persistence.Persistence interface
var _ persistence.Persistence = (*CassandraStore)(nil)

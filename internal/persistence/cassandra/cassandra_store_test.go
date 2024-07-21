package cassandra

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCassandraStoreInitialization(t *testing.T) {
	hosts := []string{"127.0.0.1"}
	keyspace := "example_keyspace"
	store, err := NewCassandraStore(hosts, keyspace, "test_table", 1)
	// var cStore CassandraStore = (*CassandraStore)(store)
	assert.NoError(t, err, "Expected no error during store initialization")
	// assert.NotNil(t, cStore.session, "Expected session to be initialized")
	store.ShutDown()
}

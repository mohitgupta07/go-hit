// internal/persistence/persistence.go

package persistence

type Persistence interface {
	SaveToDisk(key, value, op string)
	SaveAllToDisk(store map[string]string)
	Load() (map[string]string, error)
}

package persistence

import (
	"encoding/json"
	"log"
	"os"
	"sync"
)

type JSONPersistence struct {
	filePath string
	mu       sync.Mutex
}

func NewJSONPersistence(filePath string) *JSONPersistence {
	jp := &JSONPersistence{
		filePath: filePath,
	}
	return jp
}

func (jp *JSONPersistence) SaveToDisk(key, value, op string) {
	data := map[string]string{key: value}
	if op == "delete" {
		data = map[string]string{key: ""}
	}

	file, err := os.OpenFile("datastore.json", os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		log.Println("Error opening datastore file:", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	err = enc.Encode(data)
	if err != nil {
		log.Println("Error encoding data to JSON:", err)
		return
	}
}

func (jp *JSONPersistence) SaveAllToDisk(store map[string]string) {
	file, err := os.Create(jp.filePath)
	if err != nil {
		log.Println("Error creating datastore file:", err)
		return
	}
	defer file.Close()

	enc := json.NewEncoder(file)
	err = enc.Encode(store)
	if err != nil {
		log.Println("Error encoding data to JSON:", err)
		return
	}
}

func (jp *JSONPersistence) Load() (map[string]string, error) {
	jp.mu.Lock()
	defer jp.mu.Unlock()

	var data map[string]string
	file, err := os.ReadFile(jp.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return make(map[string]string), nil
		}
		return nil, err
	}

	err = json.Unmarshal(file, &data)
	if err != nil {
		return nil, err
	}

	return data, nil
}

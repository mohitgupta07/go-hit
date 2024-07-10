// cmd/go-hit-server/main.go

package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"

	"github.com/Mohitgupta07/go-hit/internal/datastore"
	"github.com/Mohitgupta07/go-hit/internal/persistence"
)

var kvStore *datastore.KeyValueStore

func init() {
	// Initialize JSONPersistence with file path
	jsonPersistence := persistence.NewJSONPersistence("datastore.json")
	// Initialize KeyValueStore with JSONPersistence
	kvStore = datastore.NewKeyValueStore(jsonPersistence)
	log.Println("Initialized Key Value Store")
}

func main() {
	http.HandleFunc("/set", setHandler)
	http.HandleFunc("/get", getHandler)
	http.HandleFunc("/delete", deleteHandler)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080" // Default port
	}

	fmt.Printf("Starting Go Redis Server on port %s...\n", port)
	log.Fatal(http.ListenAndServe(":"+port, nil))
}

func setHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("debug::SetHandler")
	var request struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	}

	err := json.NewDecoder(r.Body).Decode(&request)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	kvStore.Set(request.Key, request.Value)

	w.WriteHeader(http.StatusCreated)
	fmt.Println("debug::SetHandler" + request.Key + " " + request.Value)
}

func getHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	value := kvStore.Get(key)
	fmt.Println("debug::GetHandler" + key + " " + value)

	response := map[string]string{"key": key, "value": value}
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(jsonResponse)
}

func deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.URL.Query().Get("key")
	kvStore.Delete(key)
	fmt.Println("debug::DeleteHandler" + key)

	w.WriteHeader(http.StatusNoContent)
}

// cmd/go-hit-server/main.go

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/Mohitgupta07/go-hit/internal/datastore"
	"github.com/Mohitgupta07/go-hit/internal/persistence"
)

var kvStore *datastore.KeyValueStore

func init() {
	// Initialize persistenceObject with file path
	persistenceObject, _ := persistence.NewSFWPersistence("./datalake", 5)
	// Initialize KeyValueStore with persistenceObject
	kvStore = datastore.NewKeyValueStore(persistenceObject)
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

	// fmt.Printf("Starting Go Redis Server on port %s...\n", port)
	// log.Fatal(http.ListenAndServe(":"+port, nil))

	server := &http.Server{Addr: ":" + port}

	// Graceful shutdown handling
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		fmt.Printf("Starting Go Redis Server on port %s...\n", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Could not listen on %s: %v\n", port, err)
		}
	}()

	// Wait for interrupt signal
	<-stop

	// Shutdown server with a timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		log.Fatal("Server forced to shutdown:", err)
	}

	// Ensure all queued operations are processed
	kvStore.ShutDown()

	fmt.Println("Server exiting")
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

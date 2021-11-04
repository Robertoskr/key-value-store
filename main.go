package main

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/robertoskr/key_value"
)

var logger *key_value.FileTransactionLogger

func initializeTransactionLog() error {
	var err error
	logger, err = key_value.NewFileTransactionLogger("transaction.log")
	if err != nil {
		return fmt.Errorf("failed to create event logger: %w", err)
	}
	events, errors := logger.ReadEvents()
	fmt.Println(events)
	e, ok := key_value.Event{}, true
	for ok && err == nil {
		select {
		case err, ok = <-errors:
		case e, ok = <-events:
			switch e.EventType {
			case key_value.EventDelete:
				err = key_value.Delete(e.Key)
			case key_value.EventPut:
				err = key_value.Put(e.Key, e.Value)
			}
		}
	}

	logger.Run()
	return err
}

func keyValuePutHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("PUT request")
	vars := mux.Vars(r) //retrieve key from the request
	key := vars["key"]

	value, err := io.ReadAll(r.Body)
	fmt.Println(key, value)
	defer r.Body.Close()

	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
		return
	}
	logger.WritePut(key, string(value))
	if err != nil {
		http.Error(w,
			err.Error(),
			http.StatusInternalServerError)
	}

	w.WriteHeader(http.StatusCreated)
}

func keyValueGetHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("GET request")
	vars := mux.Vars(r)
	key := vars["key"]

	value, err := key_value.Get(key)
	if errors.Is(err, key_value.ErrorNoSuchKey) {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	w.Write([]byte(value))
}

func main() {
	r := mux.NewRouter()
	err := initializeTransactionLog()
	if err != nil {
		fmt.Println("an error ocurred while initializing the transaction log")
	}
	//register keyvalueputhandler as the handler function for put
	r.HandleFunc("/v1/{key}", keyValuePutHandler).Methods("PUT")
	r.HandleFunc("/v1/{key}", keyValueGetHandler).Methods("GET")
	log.Fatal(http.ListenAndServe(":8080", r))
}

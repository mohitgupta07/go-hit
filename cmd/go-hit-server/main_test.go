// main_test.go

package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

)

func TestSetHandler(t *testing.T) {
	reqBody, _ := json.Marshal(map[string]string{
		"key":   "foo",
		"value": "bar",
	})
	req, err := http.NewRequest("POST", "/set", bytes.NewBuffer(reqBody))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(setHandler)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusCreated {
		t.Errorf("handler returned wrong status code: got %v want %v",
			rr.Code, http.StatusCreated)
	}
}

func TestGetHandler(t *testing.T) {
	req, err := http.NewRequest("GET", "/get?key=foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(getHandler)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("handler returned wrong status code: got %v want %v",
			rr.Code, http.StatusOK)
	}

	expected := `{"key":"foo","value":"bar"}`
	if rr.Body.String() != expected {
		t.Errorf("handler returned unexpected body: got %v want %v",
			rr.Body.String(), expected)
	}
}

func TestDeleteHandler(t *testing.T) {
	req, err := http.NewRequest("DELETE", "/delete?key=foo", nil)
	if err != nil {
		t.Fatal(err)
	}

	rr := httptest.NewRecorder()
	handler := http.HandlerFunc(deleteHandler)

	handler.ServeHTTP(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("handler returned wrong status code: got %v want %v",
			rr.Code, http.StatusNoContent)
	}
}

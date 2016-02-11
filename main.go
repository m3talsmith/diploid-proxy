package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"io/ioutil"
	"log"
	"net/http"
)

// TODOS:
//  implement PUT method
//  implement DELETE method
//  make sure Couchbase is using the correct primary key or index.

var blankIdError = errors.New("Cannot insert record with blank id")

// bucket reference
var bucket *gocb.Bucket

type Response struct {
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
	Status int         `json:"status,omitempty"`
}

func main() {
	checkOptions()

	protocol := "couchbases"
	if app.Insecure {
		protocol = "couchbase"
		log.Println("[couchbase] *WARNING* Connecting to CouchBase insecurely")
	}

	couchbaseURI := fmt.Sprintf("%s://%s", protocol, app.CouchbaseHost)

	cluster, err := gocb.Connect(couchbaseURI)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("[couchbase] Connected to couchbase at %s\n", couchbaseURI)

	bucket, err = cluster.OpenBucket(app.Bucket, "")
	if err != nil {
		log.Fatal(err)
	}

	createBucketIndex := gocb.NewN1qlQuery(fmt.Sprintf("CREATE PRIMARY INDEX `key` ON `%s` USING GSI", app.Bucket))
	if _, err := bucket.ExecuteN1qlQuery(createBucketIndex, nil); err != nil {
		fmt.Println("ERROR EXECUTING N1QL QUERY for index creation")
		log.Fatal(err)
	}

	log.Printf("[couchbase] Using bucket %q", app.Bucket)

	router := mux.NewRouter()

	// add routes here
	router.HandleFunc("/health", handleHealth).Methods("GET")

	router.HandleFunc("/{docType}", handleGetMany).Methods("GET")
	router.HandleFunc("/{docType}", handlePost).Methods("POST")
	router.HandleFunc("/{docType}/{id}", handleGetSingle).Methods("GET")
	router.HandleFunc("/{docType}/{id}", handlePut).Methods("PUT")
	router.HandleFunc("/{docType}/{id}", handleDelete).Methods("DELETE")

	// init router
	http.Handle("/", router)

	port := fmt.Sprintf(":%d", app.ProxyPort)
	log.Println(fmt.Sprintf("[server] Starting server on localhost%s", port))
	http.ListenAndServe(port, nil)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	respond(w, "ok", 200)
}

func handlePost(w http.ResponseWriter, r *http.Request) {

	//params
	vars := mux.Vars(r)
	docType, _ := vars["docType"]

	// get body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	var bodyMap map[string]interface{}

	if err := json.Unmarshal(body, &bodyMap); err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	id := generateId()
	log.Printf("[debug] id => %s", id)

	bodyMap["id"] = id
	bodyMap["doc_type"] = docType
	bodyMap["key"] = generateKey(docType, id)

	saved, err := insertRecord(id, bodyMap)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	respond(w, saved, 201)
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docType, _ := vars["docType"]
	id, _ := vars["id"]

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	var foundMap map[string]interface{}

	if _, err := bucket.Get(id, &foundMap); err != nil {
		respondError(w, err.Error(), 404)
		return
	}

	var bodyMap map[string]interface{}

	if err := json.Unmarshal(body, &bodyMap); err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	bodyMap["doc_type"] = docType
	bodyMap["key"] = generateKey(docType, id)

	saved, err := updateRecord(id, foundMap, bodyMap)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	respond(w, saved, 201)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id, _ := vars["id"]

	var foundMap map[string]interface{}

	if _, err := bucket.Get(id, &foundMap); err != nil {
		respondError(w, err.Error(), 404)
		return
	}

	if _, err := bucket.Remove(id, 0); err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	respond(w, foundMap, 201)
}

func handleGetMany(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docType, _ := vars["docType"]

	queryString := fmt.Sprintf("SELECT * FROM `%s` WHERE doc_type=%q LIMIT 100;", app.Bucket, docType)
	myQuery := gocb.NewN1qlQuery(queryString)
	myQuery.Consistency(gocb.RequestPlus)

	rows, err := bucket.ExecuteN1qlQuery(myQuery, nil)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	var dataRows []interface{}
	var row interface{} // interface{} instead of map[string]interface{} here or it won't work
	for rows.Next(&row) {
		dataRows = append(dataRows, row)
	}
	_ = rows.Close()
	respond(w, dataRows, 200)
}

func handleGetSingle(w http.ResponseWriter, r *http.Request) {
	//get params
	vars := mux.Vars(r)
	// docType, _ := vars["docType"]
	id, _ := vars["id"]

	var found map[string]interface{}

	if _, err := bucket.Get(id, &found); err != nil {
		respondError(w, err.Error(), 404)
		return
	}

	respond(w, found, 200)
}

func respond(w http.ResponseWriter, data interface{}, status int) {
	resp := Response{
		Data:   data, // map[string]interface{}
		Status: status,
	}

	bytes, err := json.Marshal(&resp)
	if err != nil {
		bytes = []byte(`{"error":"failed to marshal response into json","status":500}`)
		status = 500
	}

	log.Printf("[server][%d][response] %s\n", status, string(bytes))

	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(bytes)

}

func respondError(w http.ResponseWriter, err string, status int) {
	resp := Response{
		Error:  err,
		Status: status,
	}

	bytes, failure := json.Marshal(&resp)
	if failure != nil {
		bytes = []byte(`{"error":"failed to marshal error into json","status":500}`)
		status = 500
	}

	log.Printf("[server][%d][error] %s\n", status, err)

	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(bytes)
}

func insertRecord(id string, data map[string]interface{}) (map[string]interface{}, error) {
	if id == "" {
		return data, blankIdError
	}
	_, err := bucket.Insert(id, data, 0)
	if err != nil {
		return data, err
	}
	return data, nil
}

func updateRecord(id string, resource, changes map[string]interface{}) (map[string]interface{}, error) {
	if id == "" {
		return resource, blankIdError
	}

	for key, value := range changes {
		resource[key] = value
	}

	_, err := bucket.Upsert(id, resource, 0)
	if err != nil {
		return resource, err
	}

	return resource, nil
}

func generateId() string {
	return uuid.New()
}

func generateKey(docType, id string) string {
	return fmt.Sprintf("%s::%s", docType, id)
}

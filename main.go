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
	"strconv"
)

var errBlankID = errors.New("Cannot insert record with blank id")

// bucket reference
var bucket *gocb.Bucket

// Response to http request
type Response struct {
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
	Status int         `json:"status,omitempty"`
}

func main() {
	// Set app flags from the cli
	checkOptions()

	protocol := "couchbases"
	if app.Insecure {
		protocol = "couchbase"
		if app.Verbose {
			log.Println("[couchbase] *WARNING* Connecting to CouchBase insecurely")
		}
	}

	couchbaseURI := fmt.Sprintf("%s://%s", protocol, app.CouchbaseHost)

	cluster, err := gocb.Connect(couchbaseURI)
	if err != nil {
		log.Fatal(err)
	}
	if app.Verbose {
		log.Printf("[couchbase] Connected to couchbase at %s\n", couchbaseURI)
	}

	bucket, err = cluster.OpenBucket(app.Bucket, "")
	if err != nil {
		log.Fatal(err)
	}

	createBucketIndex := gocb.NewN1qlQuery(fmt.Sprintf("CREATE PRIMARY INDEX `key` ON `%s` USING GSI", app.Bucket))
	if _, err := bucket.ExecuteN1qlQuery(createBucketIndex, nil); err != nil {
		fmt.Println("ERROR EXECUTING N1QL QUERY for index creation")
		log.Fatal(err)
	}

	if app.Verbose {
		log.Printf("[couchbase] Using bucket %q", app.Bucket)
	}

	router := mux.NewRouter()

	// add routes here
	router.HandleFunc("/health", handleHealth).Methods("GET")

	router.HandleFunc("/resource/{docType}", handleGetMany).Methods("GET")
	router.HandleFunc("/resource/{docType}", handlePost).Methods("POST")
	router.HandleFunc("/resource/{docType}/{id}", handleGetSingle).Methods("GET")
	router.HandleFunc("/resource/{docType}/{id}", handlePut).Methods("PUT")
	router.HandleFunc("/resource/{docType}/{id}", handleDelete).Methods("DELETE")
	router.HandleFunc("/view/{docType}/{id}/{viewName}", handleView).Methods("GET")

	// init router
	http.Handle("/", router)

	port := fmt.Sprintf(":%d", app.ProxyPort)
	if app.Verbose {
		log.Println(fmt.Sprintf("[server] Starting server on localhost%s", port))
	}
	http.ListenAndServe(port, nil)
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	respond(w, "ok", 200)
}

func handleView(w http.ResponseWriter, r *http.Request) {
	// While in dev the value of the Design Document Name is _design/dev_<docType>
	// i.e. _design/dev_hospital
	// the viewName should be the name of the relation
	// i.e. if a hospital has_many doctors in the schema then the
	// viewName is "doctors"

	// example route: http://localhost:4051/view/dev_hospital/doctors

	vars := mux.Vars(r)
	id := vars["id"]
	viewName := vars["viewName"]
	docType := vars["docType"]

	key := generateKey(docType, id)
	query := gocb.NewViewQuery(docType, viewName).Key(key)
	rows, err := bucket.ExecuteViewQuery(query)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	var data []interface{}
	var row map[string]interface{}
	for rows.Next(&row) {
		data = append(data, row["value"])
	}
	rows.Close()

	respond(w, data, 200)
}

func handleDevView(w http.ResponseWriter, r *http.Request) {
	// The value of the Design Document Name is _design/dev_<docType>
	// i.e. _design/dev_hospital
	// the viewName should be the name of the relation
	// i.e. if a hospital has_many doctors in the schema then the
	// viewName is "doctors"

	// vars := mux.Vars(r)
	// id := vars["id"]
	id := "acb6a1fb-59f0-4b2f-b30c-3bee0a9fd83a"
	//viewName := vars["viewName"]
	viewName := "doctors"
	docType := "hospital" // vars["docType"]
	key := generateKey(docType, id)
	designDoc := fmt.Sprintf("dev_%s", docType)
	query := gocb.NewViewQuery(designDoc, viewName).Limit(10).Key(key)

	rows, err := bucket.ExecuteViewQuery(query)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	var data = make([]interface{}, 0)
	var row interface{}
	for rows.Next(&row) {
		data = append(data, row)
	}
	rows.Close()

	respond(w, data, 200)
}

func handlePost(w http.ResponseWriter, r *http.Request) {

	//params
	vars := mux.Vars(r)
	docType := vars["docType"]

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

	bodyMap["doc_type"] = docType

	id, idExists := bodyMap["id"].(string)
	if !idExists {
		id = generateID()
		bodyMap["id"] = id
	}

	key := generateKey(docType, id)
	if app.Verbose {
		log.Printf("[debug] id => %s", id)
	}
	bodyMap["key"] = key

	saved, err := insertRecord(key, bodyMap)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	respond(w, saved, 201)
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docType := vars["docType"]
	id := vars["id"]
	key := generateKey(docType, id)

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

	var foundMap map[string]interface{}

	if _, err := bucket.Get(key, &foundMap); err != nil {
		respondError(w, err.Error(), 404)
		return
	}

	bodyMap["doc_type"] = docType
	bodyMap["key"] = key

	saved, err := updateRecord(key, foundMap, bodyMap)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	respond(w, saved, 202)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docType := vars["docType"]
	id := vars["id"]
	key := generateKey(docType, id)

	var foundMap map[string]interface{}

	if _, err := bucket.Get(key, &foundMap); err != nil {
		respondError(w, err.Error(), 404)
		return
	}

	if _, err := bucket.Remove(key, 0); err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	respond(w, foundMap, 202)
}

func handleGetMany(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	docType := vars["docType"]

	_ = r.ParseForm()

	page := r.FormValue("page")
	if page == "" {
		page = "1"
	}

	amount := r.FormValue("amount")
	if amount == "" {
		amount = "25"
	}

	pageInt, _ := strconv.Atoi(page)
	amountInt, _ := strconv.Atoi(amount)
	offset := (pageInt - 1) * amountInt

	queryString := fmt.Sprintf("SELECT * FROM `%s` WHERE doc_type=%q LIMIT %v OFFSET %v;", app.Bucket, docType, amountInt, offset)
	myQuery := gocb.NewN1qlQuery(queryString)
	myQuery.Consistency(gocb.RequestPlus)

	rows, err := bucket.ExecuteN1qlQuery(myQuery, nil)
	if err != nil {
		respondError(w, err.Error(), 500)
		return
	}

	var dataRows []interface{}
	var row map[string]interface{}
	for rows.Next(&row) {
		dataRows = append(dataRows, row[app.Bucket])
	}
	_ = rows.Close()
	respond(w, dataRows, 200)
}

func handleGetSingle(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	id := vars["id"]
	docType := vars["docType"]
	key := generateKey(docType, id)

	var found map[string]interface{}

	if _, err := bucket.Get(key, &found); err != nil {
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

	if app.Verbose {
		log.Printf("[server][%d][response] %s\n", status, string(bytes))
	}

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

	if app.Verbose {
		log.Printf("[server][%d][error] %s\n", status, err)
	}

	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(bytes)
}

func insertRecord(key string, data map[string]interface{}) (map[string]interface{}, error) {
	if key == "" {
		return data, errBlankID
	}
	_, err := bucket.Insert(key, data, 0)
	if err != nil {
		return data, err
	}
	return data, nil
}

func updateRecord(key string, resource, changes map[string]interface{}) (map[string]interface{}, error) {
	if key == "" {
		return resource, errBlankID
	}

	for key, value := range changes {
		resource[key] = value
	}

	_, err := bucket.Upsert(key, resource, 0)
	if err != nil {
		return resource, err
	}

	return resource, nil
}

func generateID() string {
	return uuid.New()
}

func generateKey(docType, id string) string {
	/* Generates a URN as a key: RFC 2141
	 * https://www.ietf.org/rfc/rfc2141.txt
	 */
	return fmt.Sprintf("%s:%s", docType, id)
}

package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/couchbase/gocb"
	"github.com/gorilla/mux"
	"github.com/pborman/uuid"
	"io/ioutil"
	"net/http"
)

var debug bool = false
var count int = 0

var blankIdError = errors.New("Cannot insert record with blank id")

// bucket reference
var bucket *gocb.Bucket

type Response struct {
	Error  string      `json:"error,omitempty"`
	Data   interface{} `json:"data,omitempty"`
	Status int         `json:"status,omitempty"`
}

func main() {
	cluster, _ := gocb.Connect("couchbase://127.0.0.1")
	bucket, _ = cluster.OpenBucket("equipment", "")
	router := mux.NewRouter()

	// add routes here

	router.HandleFunc("/{bucketName}", handleGetMany).Methods("GET")
	router.HandleFunc("/{bucketName}", handlePost).Methods("POST")
	router.HandleFunc("/{bucketName}/{id}", handleGetSingle).Methods("GET")
	router.HandleFunc("/{bucketName}/{id}", handlePut).Methods("PUT")
	router.HandleFunc("/{bucketName}/{id}", handleDelete).Methods("DELETE")

	// init router
	http.Handle("/", router)
	fmt.Print("starting server on localhost:4051\n")
	http.ListenAndServe(":4051", nil)
}

func bucketIdHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case "GET":
		handleGetSingle(w, r)
	case "PUT":
		handlePut(w, r)
	case "DELETE":
		handleDelete(w, r)
	default:
		somethingWentWrong(w, r)
	}
}

func somethingWentWrong(w http.ResponseWriter, r *http.Request) {
	respondError(w, "Method Not Allowed", 405)
}

func handlePost(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	//params
	vars := mux.Vars(r)
	bucketName, _ := vars["bucketName"]

	// get body
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		panic(err)
	}

	fmt.Printf("posted %#v\n", string(body))
	var bodyMap map[string]interface{}

	if err := json.Unmarshal(body, &bodyMap); err != nil {
		panic(err)
	}
	id := generateId(bucketName)
	// fmt.Printf("id was: %q", id)

	bodyMap["id"] = id
	saved, err := insertRecord(id, bodyMap)
	if err != nil {
		respondError(w, "failed to insert", 500)
	}

	respond(w, saved, 201)
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "handled %s", r.Method)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	fmt.Fprintf(w, "handled %s", r.Method)
}

func handleGetMany(w http.ResponseWriter, r *http.Request) {
	//vars := mux.Vars(r)
	//bucketName, _ := vars["bucketName"]

	// if debug {
	// 	fmt.Print("handleGetMany")
	// 	fmt.Printf("debug %+v \n", debug)
	// 	fmt.Printf("bucketName was %q \n", bucketName)
	// }
	// get params
	queryString := "SELECT * FROM `equipment` LIMIT 10"
	myQuery := gocb.NewN1qlQuery(queryString)
	myQuery.Consistency(gocb.RequestPlus)

	rows, err := bucket.ExecuteN1qlQuery(myQuery, nil)
	if err != nil {
		respondError(w, "error executing N1QL query", 500)
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
	bucketName, _ := vars["bucketName"]
	id, _ := vars["id"]

	requestId := generateId(bucketName)
	fmt.Printf("Request: %#v\n", requestId)
	count = count + 1
	fmt.Printf("get count: %d\n", count)
	//po man debugging
	// if debug {
	// 	fmt.Printf("debug %+v \n", debug)
	// 	fmt.Printf("bucketName was %q \n", bucketName)
	// 	fmt.Printf("id was %q \n", id)
	// }

	var found map[string]interface{}

	if _, err := bucket.Get(id, &found); err != nil {
		respondError(w, "Not Found", 404)
		return
	}

	respond(w, found, 200)
}

func respond(w http.ResponseWriter, data interface{}, status int) {
	cbResp := Response{
		Data:   data, // map[string]interface{}
		Status: status,
	}
	bytes, err := json.Marshal(&cbResp)
	if err != nil {
		bytes = []byte(`{"error":"failed to marshal response into json","status":500}`)
		status = 500
	}
	fmt.Printf("Responding: %d\n", status)
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
	fmt.Printf("Responding: %d", status)
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(bytes)
}

func insertRecord(id string, data map[string]interface{}) (map[string]interface{}, error) {
	if id != "" {
		return data, blankIdError
	}
	_, err := bucket.Insert(id, data, 0)
	if err != nil {
		return data, err
	}
	return data, nil
}

func generateId(bucketName string) string {
	id := uuid.New()
	return fmt.Sprintf("%s::%s", bucketName, id)
}

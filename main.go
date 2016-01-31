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

var debug bool = true

var blankIdError = errors.New("Cannot insert record with blank id")

// bucket reference
var bucket *gocb.Bucket

type Response struct {
	Error  string      `json:"error"`
	Data   interface{} `json:"data"`
	Status int         `json:"status"`
}

type Equipment struct {
	Name string `json:"name"`
	Id   string `json:"id"`
}

func main() {
	cluster, _ := gocb.Connect("couchbase://127.0.0.1")
	bucket, _ = cluster.OpenBucket("equipment", "")
	router := mux.NewRouter()

	// add routes here
	router.HandleFunc("/{bucketName}", bucketOnlyHandler).Methods("GET", "POST")
	router.HandleFunc("/{bucketName}/{id}", bucketIdHandler).Methods("GET", "PUT", "DELETE")

	// init router
	http.Handle("/", router)
	fmt.Print("starting server on localhost:3000\n")
	http.ListenAndServe(":3000", nil)
}

// wont get faster using a map until there are more cases
func bucketOnlyHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	switch r.Method {
	case "GET":
		handleGetMany(w, r)
	case "POST":
		handlePost(w, r)
	default:
		somethingWentWrong(w, r)
	}
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
	fmt.Printf("id was: %q", id)

	bodyMap["id"] = id
	saved, err := insertRecord(id, bodyMap)
	if err != nil {
		respondError(w, "failed to insert", 500)
	}

	respond(w, saved, 201)
}

func handlePut(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "handled %s", r.Method)
}

func handleDelete(w http.ResponseWriter, r *http.Request) {
	fmt.Fprintf(w, "handled %s", r.Method)
}

func handleGetMany(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	bucketName, _ := vars["bucketName"]

	if debug {
		fmt.Print("handleGetMany")
		fmt.Printf("debug %+v \n", debug)
		fmt.Printf("bucketName was %q \n", bucketName)
	}
	// get params
	queryString := "SELECT * FROM `equipment` LIMIT 10"
	myQuery := gocb.NewN1qlQuery(queryString)

	rows, err := bucket.ExecuteN1qlQuery(myQuery, nil)
	if err != nil {
		respondError(w, "error executing N1QL query", 500)
	}

	var dataRows []map[string]interface{}
	var row map[string]interface{}
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

	//po man debugging
	if debug {
		fmt.Printf("debug %+v \n", debug)
		fmt.Printf("bucketName was %q \n", bucketName)
		fmt.Printf("id was %q \n", id)
	}

	var found map[string]interface{}

	if _, err := bucket.Get(id, &found); err != nil {
		respondError(w, "Not Found", 404)
		return
	}

	respond(w, found, 200)
}

func respond(w http.ResponseWriter, data interface{}, status int) {
	cbResp := Response{
		Data:   data,
		Status: status,
	}
	bytes, err := json.Marshal(&cbResp)
	if err != nil {
		bytes = []byte(`{"error":"failed to marshal response into json","status":500}`)
		status = 500
	}
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

// func LoadUserFromToken(myToken string) (*User, error) {
// 	var u User
// 	token, _ := jwt.Parse(myToken, func(token *jwt.Token) (interface{}, error) {
// 		return hashToken, nil
// 	})
// 	//if err != nil {
// 	//  fmt.Println("TOKEN", token, err)
// 	//  return nil, errors.New("Token Parsing Problem")
// 	//}
// }

// func (u *User) Save() bool {
// 	if _, err := bucket.Upsert(u.Name, u, 0); err != nil {
// 		return false
// 	}
// 	return true
// }

// func (u *UserIntermediary) CreateUser() bool {
// 	token := jwt.New(jwt.SigningMethodHS256)
// 	token.Claims["user"] = u.User
// 	if encryptedToken, err := token.SignedString([]byte(hashToken)); err != nil {
// 		return false
// 	} else {
// 		u.Token = encryptedToken
// 	}

// 	var newUser User
// 	newUser.Type = "User"
// 	newUser.ID = "NOT_CURRENTLY_USED"
// 	newUser.Name = u.User
// 	newUser.Password = u.Password
// 	newUser.Token = u.Token
// 	if _, err := bucket.Insert(newUser.Name, newUser, 0); err != nil {
// 		return false
// 	}
// 	return true
// }

// func (u *UserIntermediary) LoginUser() bool {
// 	var curUser User
// 	if _, err := bucket.Get(u.User, &curUser); err != nil {
// 		return false
// 	}
// 	if u.Password == curUser.Password {
// 		u.Token = curUser.Token
// 		return true
// 	}
// 	return false
// }

// func (u *UserIntermediary) CheckUserExists() bool {
// 	var curUser User
// 	if _, err := bucket.Get(u.User, &curUser); err != nil {
// 		return false
// 	}
// 	return true
// }

// func airportHandler(w http.ResponseWriter, r *http.Request) {

// 	var queryPrep string

// 	switch search := r.URL.Query().Get("search"); len(search) {
// 	case 3:
// 		queryPrep = "SELECT airportname FROM `travel-sample` WHERE faa ='" + strings.ToUpper(search) + "'"
// 	case 4:
// 		if s := strings.ToUpper(search); s == search {
// 			queryPrep = "SELECT airportname FROM `travel-sample` WHERE icao ='" + strings.ToUpper(search) + "'"
// 		} else {
// 			queryPrep = "SELECT airportname FROM `travel-sample` WHERE airportname like '" + search + "%'"
// 		}
// 	default:
// 		queryPrep = "SELECT airportname FROM `travel-sample` WHERE airportname like '" + search + "%'"
// 	}

// 	myQuery := gocb.NewN1qlQuery(queryPrep)
// 	rows, err := bucket.ExecuteN1qlQuery(myQuery, nil)
// 	if err != nil {
// 		fmt.Println("ERROR EXECUTING N1QL QUERY:", err)
// 	}

// 	var airports []Airport
// 	var row Airport
// 	for rows.Next(&row) {
// 		airports = append(airports, row)
// 	}
// 	_ = rows.Close()
// 	bytes, _ := json.Marshal(airports)
// 	w.Write(bytes)
// }

// func flightPathHandler(w http.ResponseWriter, r *http.Request) {

// 	var queryPrep, queryTo, queryFrom string
// 	var fromLon, fromLat, toLon, toLat, dist float64
// 	var price, flightTime, weekday int
// 	var leave time.Time
// 	var row AirportIntermediary
// 	var airports []AirportIntermediary
// 	var flight Flight
// 	var flights []Flight

// 	from := r.URL.Query().Get("from")
// 	to := r.URL.Query().Get("to")

// 	leave, _ = time.Parse(layout, r.URL.Query().Get("leave"))
// 	weekday = int(leave.Weekday()) + 1

// 	queryPrep = "SELECT faa as fromAirport,geo FROM `travel-sample` WHERE airportname = '" + from +
// 		"' UNION SELECT faa as toAirport,geo FROM `travel-sample` WHERE airportname = '" + to + "'"

// 	myQuery := gocb.NewN1qlQuery(queryPrep)
// 	rows, err := bucket.ExecuteN1qlQuery(myQuery, nil)
// 	if err != nil {
// 		fmt.Println("ERROR EXECUTING N1QL QUERY:", err)
// 	}

// 	for rows.Next(&row) {
// 		airports = append(airports, row)
// 		if row.ToAirport != "" {
// 			toLat = row.Geo.Lat
// 			toLon = row.Geo.Lon
// 			queryTo = row.ToAirport
// 		}
// 		if row.FromAirport != "" {
// 			fromLat = row.Geo.Lat
// 			fromLon = row.Geo.Lon
// 			queryFrom = row.FromAirport
// 		}
// 		row = AirportIntermediary{}
// 	}
// 	dist = Haversine(fromLon, fromLat, toLon, toLat)
// 	flightTime = int(dist / averageKilometersHour)
// 	price = int(dist * distanceCostMultiplier)

// 	_ = rows.Close()

// 	queryPrep = "SELECT r.id, a.name, s.flight, s.utc, r.sourceairport, r.destinationairport, r.equipment " +
// 		"FROM `travel-sample` r UNNEST r.schedule s JOIN `travel-sample` a ON KEYS r.airlineid WHERE r.sourceairport='" +
// 		queryFrom + "' AND r.destinationairport='" + queryTo + "' AND s.day=" + strconv.Itoa(weekday) + " ORDER BY a.name"

// 	myQuery = gocb.NewN1qlQuery(queryPrep)
// 	rows, err = bucket.ExecuteN1qlQuery(myQuery, nil)
// 	if err != nil {
// 		fmt.Println("ERROR EXECUTING N1QL QUERY:", err)
// 	}

// 	for i := 0; rows.Next(&flight); i++ {
// 		flight.Flighttime = flightTime
// 		flight.Price = price
// 		flights = append(flights, flight)
// 	}
// 	_ = rows.Close()
// 	bytes, _ := json.Marshal(flights)
// 	w.Write(bytes)
// }

// func Haversine(lonFrom float64, latFrom float64, lonTo float64, latTo float64) (distance float64) {

// 	var deltaLat = (latTo - latFrom) * (math.Pi / 180)
// 	var deltaLon = (lonTo - lonFrom) * (math.Pi / 180)
// 	var a = math.Sin(deltaLat/2)*math.Sin(deltaLat/2) +
// 		math.Cos(latFrom*(math.Pi/180))*math.Cos(latTo*(math.Pi/180))*
// 			math.Sin(deltaLon/2)*math.Sin(deltaLon/2)
// 	var c = 2 * math.Atan2(math.Sqrt(a), math.Sqrt(1-a))
// 	distance = earthRadius * c
// 	return
// }

// func bucketHandler(w http.ResponseWriter, r *http.Request) {

// 	var q UserIntermediary
// 	var s struct {
// 		Success string `json:"success"`
// 	}

// 	switch r.Method {
// 	case "GET":
// 		// login request for existing user
// 		q.User = r.URL.Query().Get("user")
// 		q.Password = r.URL.Query().Get("password")
// 		if authenticated := q.LoginUser(); authenticated == true {
// 			s.Success = q.Token
// 			bytes, _ := json.Marshal(s)
// 			w.Write(bytes)
// 		} else {
// 			bytes := []byte(`{"failure":"Bad Username or Password"}`)
// 			w.Write(bytes)
// 		}
// 	case "POST":
// 		// login request for a new user
// 		_ = json.NewDecoder(r.Body).Decode(&q)
// 		if exists := q.CheckUserExists(); exists == true {
// 			bytes := []byte(`{"failure":"User exists, please choose a different username"}`)
// 			w.Write(bytes)
// 		}
// 		if created := q.CreateUser(); created == true {
// 			s.Success = q.Token
// 			bytes, _ := json.Marshal(s)
// 			w.Write(bytes)
// 		}
// 	}
// }

// func userFlightsHandler(w http.ResponseWriter, r *http.Request) {
// 	switch r.Method {
// 	case "GET":
// 		token := r.URL.Query().Get("token")
// 		if t, err := LoadUserFromToken(token); err != nil {
// 			fmt.Println("ERROR", err)
// 		} else {
// 			bytes, _ := json.Marshal(t.Flights)
// 			w.Write(bytes)
// 		}
// 	case "POST":
// 		var t *User
// 		var n UserFlight
// 		var err error
// 		var i int
// 		var f struct {
// 			Token   string           `json:"token"`
// 			Flights []InternalFlight `json:"flights"`
// 		}
// 		var s struct {
// 			Added int `json:"added"`
// 		}
// 		u := time.Now()
// 		_ = json.NewDecoder(r.Body).Decode(&f)
// 		if t, err = LoadUserFromToken(f.Token); err != nil {
// 			fmt.Println("ERROR", err)
// 		}
// 		for i = 0; i < len(f.Flights); i++ {
// 			n = f.Flights[i].Data
// 			n.Bookedon = u.Format(time.RFC3339)
// 			t.Flights = append(t.Flights, n)
// 		}
// 		if created := t.Save(); created == true {
// 			s.Added = i
// 			bytes, _ := json.Marshal(s)
// 			w.Write(bytes)
// 		}
// 	default:
// 	}
// }

// func main() {
// 	// Cluster connection and bucket for couchbase
// 	cluster, _ := gocb.Connect("couchbase://127.0.0.1")
// 	bucket, _ = cluster.OpenBucket("travel-sample", "")

// 	// Http Routing
// 	http.Handle("/", http.FileServer(http.Dir("./static")))
// 	http.HandleFunc("/api/airport/findAll", airportHandler)
// 	http.HandleFunc("/api/flightPath/findAll", flightPathHandler)
// 	http.HandleFunc("/api/user/login", loginHandler)
// 	http.HandleFunc("/api/user/flights", userFlightsHandler)
// 	fmt.Printf("Starting server on :3000\n")
// 	http.ListenAndServe(":3000", nil)
// }

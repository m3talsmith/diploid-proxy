package main

import "flag"

type App struct {
	CouchbaseHost string
	ProxyPort     int
	Insecure      bool
	Bucket        string
}

var app App = App{}

func checkOptions() {
	flag.StringVar(&app.CouchbaseHost, "host", "127.0.0.1", "Couchbase host")
	flag.StringVar(&app.CouchbaseHost, "h", "127.0.0.1", "Couchbase host")
	flag.IntVar(&app.ProxyPort, "port", 4051, "Server Proxy port")
	flag.IntVar(&app.ProxyPort, "p", 4051, "Server Proxy port")
	flag.BoolVar(&app.Insecure, "insecure", false, "Use Couchbase tls")
	flag.BoolVar(&app.Insecure, "i", false, "Use Couchbase tls")
	flag.StringVar(&app.Bucket, "bucket", "default", "Couchbase bucket to use")
	flag.StringVar(&app.Bucket, "b", "default", "Couchbase bucket to use")
	flag.Parse()
}

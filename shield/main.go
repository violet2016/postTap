package main

import (
	"flag"
	"log"
	"net/http"
	"postTap/communicator"
)

var qs *QueryMsgProcessor
var queryComm *communicator.AmqpComm
var hub *Hub
var addr = flag.String("addr", ":8080", "http service address")

func init() {
	hub = newHub()
	qs = MakeQueryMsgProcessor("template1")
	qs.Queryhub = hub
	queryComm = new(communicator.AmqpComm)
}

func main() {
	flag.Parse()
	go hub.run()
	go runServer()

	if err := queryComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return
	}
	defer queryComm.Close()

	queryComm.Receive("probe", qs)
}
func serveHome(w http.ResponseWriter, r *http.Request) {
	log.Println(r.URL)
	if r.URL.Path != "/" {
		http.Error(w, "Not found", 404)
		return
	}
	if r.Method != "GET" {
		http.Error(w, "Method not allowed", 405)
		return
	}
	http.ServeFile(w, r, "home.html")
}

func runServer() {
	http.HandleFunc("/", serveHome)
	http.HandleFunc("/ws", func(w http.ResponseWriter, r *http.Request) {
		serveWs(hub, w, r)
	})
	err := http.ListenAndServe(*addr, nil)
	if err != nil {
		log.Fatal("ListenAndServe: ", err)
	}
}

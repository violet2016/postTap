package main

import (
	"log"
	"postTap/communicator"
)

func main() {
	if err := queryComm.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return
	}
	defer queryComm.Close()

	queryComm.Receive("probe", qs)

}

var qs *QueryMsgProcessor
var queryComm *communicator.AmqpComm

func init() {
	qs = MakeQueryMsgProcessor("template1")
	queryComm = new(communicator.AmqpComm)
}

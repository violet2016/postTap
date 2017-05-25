package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://kraken:guest@10.152.10.149:7777/kraken_vhost")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(
		"hello", // name
		false,   // durable
		false,   // delete when unused
		false,   // exclusive
		false,   // no-wait
		nil,     // arguments
	)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		true,   // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	failOnError(err, "Failed to register a consumer")

	forever := make(chan bool)

	go func() {
		for d := range msgs {
			process(d.Body)
		}
	}()

	log.Printf(" [*] Waiting for messages. To exit press CTRL+C")
	<-forever
}

type QueryInfo struct {
	pid       int
	queryText string
	dbname    string
	username  string
	status    string
}

var queries = map[int]QueryInfo{}
var qstatus = map[int]int{}

const (
	submit = iota
	start
	finish
)

func setStatus(pid int, status int) {
	if _, ok := qstatus[pid]; !ok {
		qstatus[pid] = 0
	}
	if qstatus[pid] <= status {
		qstatus[pid] = status
		fmt.Println(qstatus[pid])
	}
}
func process(msg []byte) {
	smsg := string(msg)
	fields := strings.Split(smsg, "|")
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return
	}
	if pid == os.Getpid() {
		return
	}
	funcName := fields[1]

	switch funcName {
	case "ExecutorStart":
		setStatus(pid, start)
	case "ExecutorFinish":
		setStatus(pid, finish)
	case "CreateQueryDesc":
		setStatus(pid, submit)
	}
	if qstatus[pid] == submit {
		query := getQueryInfo(pid)
		queries[pid] = query
	}

}

func getQueryInfo(pid int) QueryInfo {
	query := QueryInfo{pid: pid}

	return query
}

package main

import (
	"fmt"
	"log"
	"strconv"

	"kanas/database"
	"strings"

	"github.com/streadway/amqp"
)

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func main() {
	conn, err := amqp.Dial("amqp://kraken:guest@172.17.0.4:7777/kraken_vhost")
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
var db *database.ActiveRecord
var currentPID int

func init() {
	db = new(database.ActiveRecord)
	err := db.Connect("postgres", "user=gpadmin dbname=template1 sslmode=disable")
	if err != nil {
		panic(err)
	}
	db.CleanTokens().Select("pg_backend_pid()")
	row, err := db.GetRow()
	if len(row) == 1 {
		currentPID = int(row["pg_backend_pid"].(int64))
	}
}

const (
	submit = iota
	start
	cancel
	finish
)

func setStatus(pid int, status int) {
	if _, ok := qstatus[pid]; !ok {
		qstatus[pid] = 0
	}
	if qstatus[pid] <= status {
		qstatus[pid] = status
	}
}
func getStatus(pid int) string {
	stat, ok := qstatus[pid]
	if !ok {
		return "unknown"
	}
	switch stat {
	case submit:
		return "submit"
	case start:
		return "start"
	case finish:
		return "finish"
	case cancel:
		return "cancel"
	}
	return "unknown"
}
func removeQuery(pid int) {
	delete(queries, pid)
	delete(qstatus, pid)
}
func process(msg []byte) {
	smsg := string(msg)
	fields := strings.Split(smsg, "|")
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return
	}
	if pid == currentPID {
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
	case "StatementCancelHandler":
		setStatus(pid, cancel)
	}
	var query QueryInfo
	if qstatus[pid] == submit {
		query = getQueryInfo(pid)
	} else {
		query = queries[pid]
	}
	query.status = getStatus(pid)
	queries[pid] = query
	update(pid)
	if qstatus[pid] == finish || qstatus[pid] == cancel {
		removeQuery(pid)
	}
}

func getQueryInfo(pid int) QueryInfo {
	query := QueryInfo{pid: pid}
	db.CleanTokens().Select("datname, usename, query, state").From("pg_stat_activity").Where(fmt.Sprintf("pid = %d", pid)).And("coalesce(datname, '') <> ''")
	rows, err := db.GetRows()
	if err == nil && len(rows) > 0 {
		query.dbname = rows[0]["datname"].(string)
		query.username = rows[0]["usename"].(string)
		query.queryText = rows[0]["query"].(string)
		query.status = rows[0]["state"].(string)
	} else {
		fmt.Print(err)
	}
	return query
}

func update(pid int) {
	log.Printf("%+v\n", queries[pid])
}

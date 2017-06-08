package main

import (
	"encoding/json"
	"fmt"
	"log"
	"postTap/master/pg"
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
	conn, err := amqp.Dial("amqp://guest:guest@localhost:5672")
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
	planState *PlanState
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

func parsePlanState(node string, actual bool) *PlanState {
	fields := strings.Split(node, ",")
	res := new(PlanState)
	pnodeStore := new(NodeStore)
	var err error
	for _, field := range fields {
		keyval := strings.SplitN(field, ":", 2)
		switch keyval[0] {
		case "type":
			res.NodeType, err = strconv.Atoi(keyval[1])
		case "addr":
			pnodeStore.address, err = strconv.ParseUint(keyval[1], 0, 64)
		case "plan":
			pnodeStore.planAddr, err = strconv.ParseUint(keyval[1], 0, 64)
		case "left":
			pnodeStore.leftStateAddr, err = strconv.ParseUint(keyval[1], 0, 64)
		case "right":
			pnodeStore.rightStateAddr, err = strconv.ParseUint(keyval[1], 0, 64)
		}
		if err != nil {
			log.Fatal(err)
			return nil
		}
	}
	if name, ok := pg.PlanStateString[res.NodeType]; ok {
		res.NodeTypeString = name
	}
	if actual {
		res.actualNode = pnodeStore
	} else {
		res.explainedNode = pnodeStore
	}
	return res
}

func insertIntoPlanState(ancestor *PlanState, child *PlanState, actual bool) bool {
	if ancestor == nil {
		return false
	}
	anode := ancestor.explainedNode
	cnode := child.explainedNode
	if actual {
		anode = ancestor.actualNode
		cnode = child.actualNode
	}
	if ancestor.LeftTree == nil && anode.leftStateAddr == cnode.address {
		ancestor.LeftTree = child
		return true
	}
	if ancestor.RightTree == nil && anode.rightStateAddr == cnode.address {
		ancestor.RightTree = child
		return true
	}
	if insertIntoPlanState(ancestor.LeftTree, child, actual) {
		return true
	}
	return insertIntoPlanState(ancestor.RightTree, child, actual)

}
func setActualPlan(pid int, node string) {
	state := parsePlanState(node, true)
	if q, ok := queries[pid]; ok {
		if q.planState == nil {
			q.planState = state
		} else {
			insertIntoPlanState(q.planState, state, true)
		}
		queries[pid] = q
	}
}
func setExplainedPlan(pid int, node string) {
	state := parsePlanState(node, false)
	if q, ok := queries[pid]; ok {
		if q.planState == nil {
			q.planState = state
		} else {
			insertIntoPlanState(q.planState, state, false)
		}
		queries[pid] = q
	}
}

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
	funcName := fields[1]
	if pid == currentPID && funcName != "ExplainNode" {
		return
	}
	if pid != currentPID && funcName == "ExplainNode" {
		return
	}

	switch funcName {
	case "ExecutorStart":
		setStatus(pid, start)
	case "ExecutorFinish":
		setStatus(pid, finish)
	case "CreateQueryDesc":
		setStatus(pid, submit)
	case "StatementCancelHandler":
		setStatus(pid, cancel)
	case "ExecInitNode":
		setInitPlan(pid, fields[2])
	case "ExecProcNode":
		setActualPlan(pid, fields[2])
		return
	case "ExplainNode":
		setExplainedPlan(pid, fields[2])
		return
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
		printPlan(pid)
		removeQuery(pid)
	}
}
func printPlan(pid int) {
	if q, ok := queries[pid]; ok {
		fmt.Printf("%+v\n", q.planState)
		bytes, err := json.MarshalIndent(q.planState, "", "\t")
		if err != nil {
			return
		}
		fmt.Println(string(bytes))
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
		//	db.CleanTokens().ExecSQL(fmt.Sprintf("explain %s", query.dbname))
	} else {
		fmt.Print(err)
	}
	return query
}

func update(pid int) {
	log.Printf("%+v\n", queries[pid])
}

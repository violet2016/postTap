package main

import (
	"encoding/json"
	"fmt"
	"kanas/database"
	"log"
	"postTap/communicator"
	"postTap/shield/pg"
	"strconv"
	"time"
)

// Query status
const (
	submit = iota
	start
	cancel
	finish
)

type QueryInfo struct {
	pid           int
	queryText     string
	dbname        string
	username      string
	status        string
	statusCode    int
	planStateRoot *pg.PlanStateWrapper
}

func (qi *QueryInfo) UpdatePlanStateTree(node *pg.PlanStateWrapper) {
	if qi.planStateRoot != nil {
		qi.planStateRoot.InsertNewNode(node)
	} else {
		qi.planStateRoot = node
	}
}

func (qi *QueryInfo) UpdateNode(msg string) {
	info := pg.ParsePlanString(msg)
	if plan, ok := info["plannode"]; ok {
		addr, err := strconv.ParseUint(plan, 0, 64)
		if err != nil {
			return
		}
		qs := qi.planStateRoot.FindNodeByAddr(addr)
		if qs == nil {
			return
		}
		qs.UpdateInfo(info)
	}
}

func (qi *QueryInfo) StatusChanged(stat int) {
	switch stat {
	case start:
		log.Println("start polling ...")
		go qi.StartPolling()
	case finish:
	case cancel:
	}
}

func (qi *QueryInfo) StartPolling() {
	for {
		<-time.After(15 * time.Second)

		if qi.statusCode == start && qi.planStateRoot != nil {
			command := new(communicator.Command)
			command.CommandName = "RUN"
			command.Pid = qi.pid
			script, err := qi.planStateRoot.GenExecProcNodeScript()
			if err == nil {
				command.Script = script
				msg, _ := json.Marshal(command)
				log.Println("send command")
				queryComm.Send("command", msg)
			}
		} else {
			return
		}
	}
}

func (qi *QueryInfo) EndPolling() {

}
func (qi *QueryInfo) PrintPlan() {
	bytes, err := json.MarshalIndent(qi.planStateRoot, "", "\t")
	if err != nil {
		return
	}
	fmt.Println(string(bytes))
}

func GetStatusString(stat int) string {
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

type DBWrapper struct {
	db        *database.ActiveRecord
	dbconnPID int
}

func (dbw *DBWrapper) Init(user string, dbname string) {
	dbw.db = new(database.ActiveRecord)
	err := dbw.db.Connect("postgres", fmt.Sprintf("user=%s dbname=%s sslmode=disable", user, dbname))
	if err != nil {
		panic(err)
	}
	dbw.db.CleanTokens().Select("pg_backend_pid()")
	row, err := dbw.db.GetRow()
	if len(row) == 1 {
		dbw.dbconnPID = int(row["pg_backend_pid"].(int64))
	}
}
func (dbw *DBWrapper) Close() int {
	retval := 0
	if dbw.db != nil {
		dbw.db.Close()
		retval = dbw.dbconnPID
	}
	dbw.db = nil
	dbw.dbconnPID = 0
	return retval
}
func (dbw *DBWrapper) GetPID() int {
	return dbw.dbconnPID
}

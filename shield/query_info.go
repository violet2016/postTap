package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"kanas/database"
	"log"
	"os"
	"path"
	"postTap/common"
	"postTap/communicator"
	"postTap/shield/pg"
	"strconv"
	"strings"
	"sync"
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
	instruConfig  map[string]bool
	planStateRoot *pg.PlanStateWrapper
	rwlock        sync.RWMutex
}

func (qi *QueryInfo) UpdatePlanStateTree(node *pg.PlanStateWrapper) {
	if qi.planStateRoot != nil {
		qi.rwlock.Lock()
		defer qi.rwlock.Unlock()
		qi.planStateRoot.InsertNewNode(node)
	} else {
		qi.planStateRoot = node
	}
}

func (qi *QueryInfo) UpdateNode(msg string) {
	qi.rwlock.Lock()
	defer qi.rwlock.Unlock()
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
		go qi.StartPolling()
	case finish:
	case cancel:
		go qi.EndPolling()
	}
}

func (qi *QueryInfo) SendCommand(name string) error {
	command := new(communicator.CommandMsg)
	command.CommandName = "RUN"
	command.Pid = qi.pid
	commandQueue := new(communicator.AmqpComm)
	if err := commandQueue.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return err
	}
	defer commandQueue.Close()
	qi.rwlock.RLock()
	defer qi.rwlock.RUnlock()
	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := path.Dir(ex)
	filepath := path.Join(exPath, "exec_proc_node.template")
	if qi.statusCode == start && qi.planStateRoot != nil {
		script, err := qi.GenExecProcNodeScript(filepath)
		if err == nil {
			command.Script = script
			msg, _ := json.Marshal(command)
			log.Println("Send Run Command")

			commandQueue.Send("command", msg)
		}
	} else {
		return fmt.Errorf("Query already stopped")
	}
	return nil
}

// GenExecProcNodeScript generate stap script
func (qi *QueryInfo) GenExecProcNodeScript(template string) ([]byte, error) {
	if qi.planStateRoot == nil {
		return []byte{}, nil
	}
	bfile, err := ioutil.ReadFile(template)
	if err != nil {
		return []byte{}, err
	}

	codelines := qi.planStateRoot.TranverseGenSTAP(pg.PrintInstrument)
	replaceall := bytes.Replace(bfile, []byte("PLACEHOLDER_ADDR"), codelines, -1)
	printString, addrString := qi.GenHelperFunc()
	replaceall = bytes.Replace(replaceall, []byte("PLACEHOLDER_PRINTSTRING"), []byte(printString), -1)
	replaceall = bytes.Replace(replaceall, []byte("PLACEHOLDER_MEMBER"), []byte(addrString), -1)
	replaceall = bytes.Replace(replaceall, []byte("PLACEHOLDER_POSTGRES"), common.Which("postgres"), -1)
	if len(codelines) > 0 {
		return replaceall, nil
	}
	return []byte{}, nil
}

func (qi *QueryInfo) GenHelperFunc() (string, string) {
	printCandidates := []string{}
	addrCandidates := []string{}
	for k, v := range qi.instruConfig {
		if v == true {
			if printmember, ok := pg.InstrumentMember[k]; ok {
				for name, offset := range printmember {
					printCandidates = append(printCandidates, fmt.Sprintf("%s:%s", name, "%p"))
					addrCandidates = append(addrCandidates, fmt.Sprintf("user_long(instr+%d)", offset))
				}
			}
		}
	}
	return strings.Join(printCandidates, ","), strings.Join(addrCandidates, ",")
}
func (qi *QueryInfo) StartPolling() {
	ticker := time.NewTicker(30 * time.Second)
	quitpolling := make(chan struct{})
	go func() {
		for {
			select {
			case <-ticker.C:
				if err := qi.SendCommand("RUN"); err != nil {
					close(quitpolling)
				}
			case <-quitpolling:
				ticker.Stop()
				return
			}
		}
	}()
}

func (qi *QueryInfo) EndPolling() {
	command := new(communicator.CommandMsg)
	command.CommandName = "STOP"
	command.Pid = qi.pid
	commandQueue := new(communicator.AmqpComm)
	if err := commandQueue.Connect("amqp://guest:guest@localhost:5672"); err != nil {
		log.Fatalf("%s", err)
		return
	}
	defer commandQueue.Close()
	msg, _ := json.Marshal(command)
	log.Println("Send Stop Command")
	commandQueue.Send("command", msg)

}

// PrintPlan print out the plan json to stdout
func (qi *QueryInfo) PrintPlan() {
	bytes := qi.GetPlanJSON()
	fmt.Println(string(bytes))
}

// GetPlanJSON gets the json bytes for plan
func (qi *QueryInfo) GetPlanJSON() []byte {
	qi.rwlock.RLock()
	defer qi.rwlock.RUnlock()
	bytes, err := json.MarshalIndent(qi.planStateRoot, "", "  ")
	if err != nil {
		log.Fatal(err)

	}
	return bytes
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

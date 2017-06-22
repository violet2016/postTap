package main

import (
	"fmt"
	"log"

	"postTap/shield/pg"

	"strconv"
	"strings"
)

type PlanMessage struct {
	pid  int
	body []byte
}
type QueryMsgProcessor struct {
	backendDB *DBWrapper
	Queries   map[int]*QueryInfo
	Queryhub  *Hub
}

func MakeQueryMsgProcessor(dbname string) *QueryMsgProcessor {
	qs := new(QueryMsgProcessor)
	qs.backendDB = new(DBWrapper)
	qs.backendDB.Init("gpadmin", dbname)
	qs.Queries = map[int]*QueryInfo{}
	return qs
}
func (qs *QueryMsgProcessor) DeleteQuery(pid int) {
	delete(qs.Queries, pid)
}
func (qs *QueryMsgProcessor) UpdateStatus(pid int, stat int) {
	if q, ok := qs.Queries[pid]; ok {
		if q.statusCode < stat {
			q.statusCode = stat
			q.status = GetStatusString(stat)
			log.Println("query status:", q.status)
			q.StatusChanged(stat)
		}
	} else {
		qs.Queries[pid] = &QueryInfo{pid: pid, statusCode: stat, status: GetStatusString(stat), instruConfig: map[string]bool{"base": true, "accumulated": true, "buffer": false}}
	}
	if stat == finish || stat == cancel {
		//qs.Queries[pid].PrintPlan()
		qs.DeleteQuery(pid)
	}

}

func (qs *QueryMsgProcessor) UpdateInstrument(pid int, instru string) {
	if qi, ok := qs.Queries[pid]; ok {
		qi.UpdateNode(instru)
	}
}

func (qs *QueryMsgProcessor) Export(pid int) {
	if qi, ok := qs.Queries[pid]; ok {
		//queryComm.Send("publish", qi.GetPlanJSON())
		qs.Queryhub.broadcast <- &PlanMessage{pid: pid, body: qi.GetPlanJSON()}
	}
}
func (qs *QueryMsgProcessor) IsQueryExist(pid int) bool {
	if _, ok := qs.Queries[pid]; ok {
		return true
	}
	return false
}
func (qs *QueryMsgProcessor) GetQueryDetails(pid int) error {
	qs.backendDB.db.CleanTokens().Select("datname, usename, query, state").From("pg_stat_activity").Where(fmt.Sprintf("pid = %d", pid)).And("coalesce(datname, '') <> ''")
	rows, err := qs.backendDB.db.GetRows()
	query := qs.Queries[pid]
	if err != nil {
		return err
	}
	if len(rows) > 0 {
		query.dbname = rows[0]["datname"].(string)
		query.username = rows[0]["usename"].(string)
		query.queryText = rows[0]["query"].(string)
		query.status = rows[0]["state"].(string)
		qs.Queries[pid] = query
	} else {
		return fmt.Errorf("Query not found")
	}
	return nil
}

// InitPlan with "Plan" Node msg
// Every ExecInitPlan is a new plan node
func (qs *QueryMsgProcessor) InitPlan(pid int, msg string) {
	if qi, ok := qs.Queries[pid]; ok {
		planstate := new(pg.PlanStateWrapper)
		planstate.InitPlanStateWrapperFromExecInitPlan(msg)
		qi.UpdatePlanStateTree(planstate)
	}

}

func (qs *QueryMsgProcessor) Process(msg []byte) error {
	smsg := string(msg)
	fields := strings.Split(smsg, "|")
	if len(fields) < 2 {
		return fmt.Errorf("Unspported msg type: %s", smsg)
	}
	pid, err := strconv.Atoi(fields[0])
	if err != nil {
		return fmt.Errorf("Unspported msg type: %s", smsg)
	}
	funcName := fields[1]
	switch funcName {
	case "EndInstrument":
		qs.Export(pid)
	case "GenerateNode":
		qs.UpdateStatus(pid, start)
		if len(fields) > 2 {
			qs.InitPlan(pid, fields[2])
		}
	case "ExecutorFinish":
		qs.UpdateStatus(pid, finish)
	case "CreateQueryDesc":
		qs.UpdateStatus(pid, submit)
	case "StatementCancelHandler":
		qs.UpdateStatus(pid, cancel)
	case "GetInstrument":
		if len(fields) > 2 {
			qs.UpdateInstrument(pid, fields[2])
		}
	}
	return nil
}

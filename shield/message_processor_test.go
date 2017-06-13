package main

import (
	"testing"
)

func TestProcessQuerySleep(t *testing.T) {
	msgsubmit := []byte("96700|CreateQueryDesc")
	qs.Process(msgsubmit)
	if q, ok := qs.Queries[96700]; !ok {
		t.Error("query not created")
	} else {
		if q.status != "submit" {
			t.Error("query status is not correct")
		}
	}
	msgstart := []byte("96700|ExecutorStart")
	qs.Process(msgstart)
	q, _ := qs.Queries[96700]
	if q.status != "start" {
		t.Error("query status is not correct")
	}
	msgplan := []byte("96700|ExecInitNode|plantype:124,plan:0x1aeb180,plan_rows:0x412e848000000000,leftplan:0x1aea8b8,rightplan:0x1aeb0f0")
	qs.Process(msgplan)
	q, _ = qs.Queries[96700]
	if q.planStateRoot.PlanNodeType != 124 {
		t.Error("Plan type is not correct")
	}

	msgplan = []byte("96700|ExecInitNode|plantype:109,plan:0x1aea8b8,plan_rows:0x408f400000000000,leftplan:0x0,rightplan:0x0")
	qs.Process(msgplan)
	q, _ = qs.Queries[96700]
	if q.planStateRoot.LeftTree == nil {
		t.Error("left tree Plan is null")
	}

	msgInstru := []byte("96700|GetInstrument|plannode:0x1aea8b8,instrument:0x1111,tuplecount:0x408f400000000000")
	qs.Process(msgInstru)
	if q.planStateRoot.LeftTree.Instrument != 4369 {
		t.Error("Instrument not right")
	}
	if q.planStateRoot.Instrument != 0 {
		t.Error("Root Instrument should be 0")
	}
	if q.planStateRoot.LeftTree.TupleCount != 1000 {
		t.Error("tuplecount not right")
	}
}

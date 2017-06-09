package pg

import "testing"

func TestParsePlanString(t *testing.T) {
	res := ParsePlanString("plantype:117,addr:0x1ae4630,left:0x0,right:0x0")
	if res["plantype"] != "117" {
		t.Error("address parse error")
	}
}
func TestGeneratePlanState(t *testing.T) {
	ps := new(PlanStateWrapper)
	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1ae4630,plan_rows:0x408f400000000000,left:0x0,right:0x0,estate:0x121212")
	if ps.PlanNodeType != 117 {
		t.Errorf("plan node type parse error %d", ps.PlanNodeType)
	}
	if ps.Plan.Address != 28198448 {
		t.Error("plan addr parse error")
	}
	if ps.PlanRows != 1000.0 {
		t.Error("plan rows error")
	}
	if ps.Estate.Address != 1184274 {
		t.Error("estate address error")
	}
}
func TestAllAddr(t *testing.T) {
	ps := new(PlanStateWrapper)
	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1ae4630,plan_rows:0x408f400000000000,left:0x1234,right:0x0,estate:0x121212")
	if ps.AllNodeAddrMap[28198448] != false {
		t.Error("All node addr not correct")
	}
	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1234,plan_rows:0x408f400000000000,left:0x0,right:0x0,estate:0x11")
	if ps.AllNodeAddrMap[4660] != false {
		t.Error("All node addr not correct")
	}
}
func TestConvertHexToFloat64(t *testing.T) {
	res, _ := ConvertHexToFloat64("408f400000000000")
	if res != 1000.0 {
		t.Errorf("fail convert hex to float64 %f", res)
	}
}

func TestExecProcNodeScript(t *testing.T) {
	ps := new(PlanStateWrapper)
	ps.AllNodeAddrMap = map[uint64]bool{}
	ps.AllNodeAddrMap[18446744073709551615] = false
	ps.AllNodeAddrMap[1234] = true
	_, err := ps.GenExecProcNodeScript()
	if err != nil {
		t.Error("error occurred:", err)
	}
}

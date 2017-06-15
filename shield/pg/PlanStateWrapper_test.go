package pg

import (
	"os"
	"path"
	"testing"
)

func TestParsePlanString(t *testing.T) {
	res := ParsePlanString("plantype:117,addr:0x1ae4630,leftplan:0x0,rightplan:0x0")
	if res["plantype"] != "117" {
		t.Error("address parse error")
	}
}
func TestGeneratePlanState(t *testing.T) {
	ps := new(PlanStateWrapper)
	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1ae4630,plan_rows:0x408f400000000000,leftplan:0x0,rightplan:0x0")
	if ps.PlanNodeType != 117 {
		t.Errorf("plan node type parse error %d", ps.PlanNodeType)
	}
	if ps.Plan.Address != 28198448 {
		t.Error("plan addr parse error")
	}
	if ps.PlanRows != 1000.0 {
		t.Error("plan rows error")
	}
}
func TestInstrument(t *testing.T) {
	ps := new(PlanStateWrapper)
	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1ae4630,plan_rows:0x408f400000000000,leftplan:0x1234,rightplan:0x0")

	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1234,plan_rows:0x408f400000000000,leftplan:0x0,rightplan:0x0")
	if ps.Instrument != 0 {
		t.Error("Instrument addr correct")
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
	ps.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1ae4630,plan_rows:0x408f400000000000,leftplan:0x1234,rightplan:0x0")
	sub := new(PlanStateWrapper)
	sub.InitPlanStateWrapperFromExecInitPlan("plantype:117,plan:0x1234,plan_rows:0x408f400000000000,leftplan:0x0,rightplan:0x0")
	ps.InsertNewNode(sub)

	gopath := os.Getenv("GOPATH")
	f := path.Join(gopath, "src", "postTap/shield", "exec_proc_node.template")
	_, err := ps.GenExecProcNodeScript(f)
	if err != nil {
		t.Error("error occurred:", err)
	}
}

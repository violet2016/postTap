package pg

import "testing"

func TestParsePlanString(t *testing.T) {
	res := ParsePlanString("type:117,addr:0x1ae4630,left:0x0,right:0x0")
	if res["type"] != "117" {
		t.Error("address parse error")
	}
}
func TestGeneratePlanState(t *testing.T) {
	ps := new(PlanStateWrapper)
	ps.GeneratePlanState(ParsePlanString("plantype:117,plan:0x1ae4630,plan_rows:0x408f400000000000,left:0x0,right:0x0"), true)
	if ps.PlanNodeType != 117 {
		t.Errorf("plan node type parse error %d", ps.PlanNodeType)
	}
	if ps.actualNode.planAddr != 28198448 {
		t.Error("plan addr parse error")
	}
	if ps.PlanRows != 1000.0 {
		t.Error("plan rows error")
	}
}

func TestConvertHexToFloat64(t *testing.T) {
	res := ConvertHexToFloat64("408f400000000000")
	if res != 1000.0 {
		t.Errorf("fail convert hex to float64 %f", res)
	}
}

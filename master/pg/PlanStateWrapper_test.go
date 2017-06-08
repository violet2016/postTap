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
	ps.GeneratePlanState(ParsePlanString("plantype:117,plan:0x1ae4630,left:0x0,right:0x0"), true)
	if ps.planNodeType != 117 {
		t.Errorf("plan node type parse error %d", ps.planNodeType)
	}
	if ps.actualNode.planAddr != 28198448 {
		t.Error("plan addr parse error")
	}
}

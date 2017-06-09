package pg

import (
	"log"
	"math"
	"strconv"
	"strings"
)

type NodeStore struct {
	address        uint64
	planAddr       uint64
	leftPlanAddr   uint64
	rightPlanAddr  uint64
	leftStateAddr  uint64
	rightStateAddr uint64
}
type PlanStateWrapper struct {
	NodeType       int `json:"type id"`
	PlanNodeType   int
	NodeTypeString string            `json:"type"`
	LeftTree       *PlanStateWrapper `json:"left"`
	RightTree      *PlanStateWrapper `json:"right"`
	actualNode     *NodeStore
	explainedNode  *NodeStore
	PlanRows       float64
}

const (
	NotmatchNodeStore = iota
	ActualNodeStore
	ExplainNodeStore
	PlanOnlyNodeStore
)

func (ps *PlanStateWrapper) GeneratePlanState(plan map[string]string, exec bool) {
	pnodeStore := new(NodeStore)
	var err error
	for key, val := range plan {
		switch key {
		case "plantype":
			ps.PlanNodeType, err = strconv.Atoi(val)
		case "statetype":
			ps.NodeType, err = strconv.Atoi(val)
		case "addr":
			pnodeStore.address, err = strconv.ParseUint(val, 0, 64)
		case "plan":
			pnodeStore.planAddr, err = strconv.ParseUint(val, 0, 64)
		case "left":
			pnodeStore.leftStateAddr, err = strconv.ParseUint(val, 0, 64)
		case "right":
			pnodeStore.rightStateAddr, err = strconv.ParseUint(val, 0, 64)
		case "leftplan":
			pnodeStore.leftPlanAddr, err = strconv.ParseUint(val, 0, 64)
		case "rightplan":
			pnodeStore.rightPlanAddr, err = strconv.ParseUint(val, 0, 64)
		case "plan_rows":
			ps.PlanRows = ConvertHexToFloat64(val[2:])
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if ps.NodeType != 0 {
		ps.NodeTypeString = GetNodeTypeString(ps.NodeType)
	} else if ps.PlanNodeType != 0 {
		ps.NodeTypeString = GetNodeTypeString(ps.PlanNodeType + 100)
	}
	if exec {
		ps.actualNode = pnodeStore
	} else {
		ps.explainedNode = pnodeStore
	}
}

func ConvertHexToFloat64(val string) float64 {

	n, err := strconv.ParseUint(val, 16, 64)
	if err != nil {
		panic(err)
	}
	n2 := uint64(n)
	return math.Float64frombits(n2)
}

func (ps *PlanStateWrapper) IsLeftChild(child *PlanStateWrapper) int {
	if ps.actualNode != nil && ps.actualNode.leftStateAddr != 0 &&
		ps.actualNode.leftStateAddr == child.actualNode.address {
		return ActualNodeStore
	}
	if ps.explainedNode != nil && ps.explainedNode.leftStateAddr != 0 &&
		ps.explainedNode.leftStateAddr == child.explainedNode.address {
		return ExplainNodeStore
	}
	if ps.actualNode != nil && ps.actualNode.leftPlanAddr != 0 &&
		ps.actualNode.leftPlanAddr == child.actualNode.planAddr {
		return PlanOnlyNodeStore
	}
	return NotmatchNodeStore
}

func (ps *PlanStateWrapper) IsRightChild(child *PlanStateWrapper) int {
	if ps.actualNode != nil && ps.actualNode.rightStateAddr != 0 &&
		ps.actualNode.rightStateAddr == child.actualNode.address {
		return ActualNodeStore
	}
	if ps.explainedNode != nil && ps.explainedNode.rightStateAddr != 0 &&
		ps.explainedNode.rightStateAddr == child.explainedNode.address {
		return ExplainNodeStore
	}
	if ps.actualNode != nil && ps.actualNode.rightPlanAddr != 0 &&
		ps.actualNode.rightPlanAddr == child.actualNode.planAddr {
		return PlanOnlyNodeStore
	}
	return NotmatchNodeStore
}

func (ps *PlanStateWrapper) InsertNewNode(node *PlanStateWrapper) bool {
	if ps == nil {
		return false
	}
	if ps.LeftTree == nil && ps.IsLeftChild(node) != NotmatchNodeStore {
		ps.LeftTree = node
		return true
	}
	if ps.RightTree == nil && ps.IsRightChild(node) != NotmatchNodeStore {
		ps.RightTree = node
		return true
	}
	if ps.LeftTree != nil && ps.LeftTree.InsertNewNode(node) {
		return true
	}
	if ps.RightTree != nil && ps.RightTree.InsertNewNode(node) {
		return true
	}
	return false
}

func ParsePlanString(p string) map[string]string {
	result := map[string]string{}
	fields := strings.Split(p, ",")
	for _, field := range fields {
		keyval := strings.SplitN(field, ":", 2)
		result[keyval[0]] = keyval[1]
	}
	return result
}

func (ps *PlanStateWrapper) InitPlanStateWrapperFromExecInitPlan(msg string) {
	ps.GeneratePlanState(ParsePlanString(msg), true)
}

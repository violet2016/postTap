package pg

import (
	"log"
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
	planNodeType   int
	NodeTypeString string            `json:"type"`
	LeftTree       *PlanStateWrapper `json:"left"`
	RightTree      *PlanStateWrapper `json:"right"`
	actualNode     *NodeStore
	explainedNode  *NodeStore
}

func (ps *PlanStateWrapper) GeneratePlanState(plan map[string]string, exec bool) {
	pnodeStore := new(NodeStore)
	var err error
	for key, val := range plan {
		switch key {
		case "plantype":
			ps.planNodeType, err = strconv.Atoi(val)
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
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if exec {
		ps.actualNode = pnodeStore
	} else {
		ps.explainedNode = pnodeStore
	}
}

func (ps *PlanStateWrapper) IsLeftChild(child *PlanStateWrapper) bool {
	if ps.actualNode.leftStateAddr == child.actualNode.address {
		return true
	}
	if ps.explainedNode.leftStateAddr == child.explainedNode.address {
		return true
	}
	if ps.actualNode.leftPlanAddr == child.actualNode.planAddr {
		return true
	}
	return false
}

func (ps *PlanStateWrapper) IsRightChild(child *PlanStateWrapper) bool {
	if ps.actualNode.rightStateAddr == child.actualNode.address {
		return true
	}
	if ps.explainedNode.rightStateAddr == child.explainedNode.address {
		return true
	}
	if ps.actualNode.rightPlanAddr == child.actualNode.planAddr {
		return true
	}
	return false
}

func (ps *PlanStateWrapper) InsertNewNode(node *PlanStateWrapper) bool {
	if ps.LeftTree == nil && ps.IsLeftChild(node) {
		ps.LeftTree = node
		return true
	}
	if ps.RightTree == nil && ps.IsRightChild(node) {
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

package pg

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"path"
	"runtime"
	"strconv"
	"strings"
)

// NodeStore: node address information
type NodeStore struct {
	Address   uint64
	leftAddr  uint64
	rightAddr uint64
}
type PlanStateWrapper struct {
	PlanNodeType   int               `json:"type id"`
	NodeTypeString string            `json:"type"`
	LeftTree       *PlanStateWrapper `json:"left"`
	RightTree      *PlanStateWrapper `json:"right"`
	Plan           *NodeStore
	Estate         *NodeStore
	PlanRows       float64
	Finished       bool
	EsProcessed    uint64
	AllNodeAddr    []uint64
}

func (ps *PlanStateWrapper) GeneratePlanState(plan map[string]string) uint64 {
	pnodeStore := new(NodeStore)
	enodeStore := new(NodeStore)
	var err error
	for key, val := range plan {
		switch key {
		case "plantype":
			ps.PlanNodeType, err = strconv.Atoi(val)
		case "plan":
			pnodeStore.Address, err = strconv.ParseUint(val, 0, 64)
		case "estate":
			enodeStore.Address, err = strconv.ParseUint(val, 0, 64)
		case "leftplan":
			pnodeStore.leftAddr, err = strconv.ParseUint(val, 0, 64)
		case "rightplan":
			pnodeStore.rightAddr, err = strconv.ParseUint(val, 0, 64)
		case "plan_rows":
			ps.PlanRows, err = ConvertHexToFloat64(val[2:])
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if ps.PlanNodeType != 0 {
		ps.NodeTypeString = GetNodeTypeString(ps.PlanNodeType)
	}

	ps.Estate = enodeStore
	ps.Plan = pnodeStore
	return ps.Plan.Address
}

func ConvertHexToFloat64(val string) (float64, error) {
	n, err := strconv.ParseUint(val, 16, 64)
	if err != nil {
		return 0, err
	}
	n2 := uint64(n)
	return math.Float64frombits(n2), nil

}

func (ps *PlanStateWrapper) IsLeftChild(child *PlanStateWrapper) bool {
	if ps.Plan != nil && ps.Plan.leftAddr != 0 &&
		ps.Plan.leftAddr == child.Plan.Address {
		return true
	}

	return false
}

func (ps *PlanStateWrapper) IsRightChild(child *PlanStateWrapper) bool {
	if ps.Plan != nil && ps.Plan.rightAddr != 0 &&
		ps.Plan.rightAddr == child.Plan.Address {
		return true
	}

	return false
}

func (ps *PlanStateWrapper) InsertNewNode(node *PlanStateWrapper) bool {
	if ps == nil {
		return false
	}
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

func (ps *PlanStateWrapper) InitPlanStateWrapperFromExecInitPlan(msg string) {
	ps.AllNodeAddr = append(ps.AllNodeAddr, ps.GeneratePlanState(ParsePlanString(msg)))
}

func (ps *PlanStateWrapper) GenExecProcNodeScript() ([]byte, error) {
	_, filename, _, ok := runtime.Caller(1)
	if !ok {
		return []byte{}, fmt.Errorf("Cannot open template")
	}
	filepath := path.Join(path.Dir(filename), "../exec_proc_node.template")
	bfile, err := ioutil.ReadFile(filepath)
	if err != nil {
		return []byte{}, err
	}
	codelines := []byte{}
	for _, addr := range ps.AllNodeAddr {
		codeline := []byte(fmt.Sprintf("\tprintdln(\"|\", lpid, \"ExecProcNode\", parse_estate(%d))\n", addr))
		codelines = append(codelines, codeline...)
	}
	replaceall := bytes.Replace(bfile, []byte("PLACEHOLDER"), codelines, -1)
	return replaceall, nil
}

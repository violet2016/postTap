package pg

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"path"
	"strconv"
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
	Instrument     uint64
	PlanRows       float64
	TupleCount     float64
	allInstrReady  bool
}

type PlanFunction func(*PlanStateWrapper) interface{}

func PrintInstrument(ps *PlanStateWrapper) interface{} {
	if ps.Plan.Address == 0 || ps.Instrument == 0 {
		return []byte{}
	}
	codeline := []byte(fmt.Sprintf("\t\tprintdln(\"|\", pid(), \"GetInstrument\", parse_instrument(%d, %d))\n", ps.Plan.Address, ps.Instrument))
	return codeline
}

func PrintPlan(ps *PlanStateWrapper) interface{} {
	if ps.Instrument != 0 || ps.Plan.Address == 0 {
		return []byte{}
	}
	codeline := []byte(fmt.Sprintf(`
	else if (plannode == %d) {
		instr = user_long(planstate+24)
		printdln("|", pid(), "GetInstrument", parse_instrument(plannode,instr))
	}
	`, ps.Plan.Address))
	return codeline
}

// GeneratePlanState Initilize the planstate and some info we can get during plan
func (ps *PlanStateWrapper) GeneratePlanState(plan map[string]string) uint64 {
	pnodeStore := new(NodeStore)
	var err error
	for key, val := range plan {
		switch key {
		case "plantype":
			ps.PlanNodeType, err = strconv.Atoi(val)
		case "plan":
			pnodeStore.Address, err = strconv.ParseUint(val, 0, 64)
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

	ps.Plan = pnodeStore
	ps.allInstrReady = false
	return ps.Plan.Address
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

func (ps *PlanStateWrapper) FindNodeByAddr(addr uint64) *PlanStateWrapper {
	if addr == 0 {
		return nil
	}
	if ps.Plan.Address == addr {
		return ps
	}
	if leftRes := ps.LeftTree.FindNodeByAddr(addr); leftRes != nil {
		return leftRes
	}
	return ps.RightTree.FindNodeByAddr(addr)
}

func (ps *PlanStateWrapper) InitPlanStateWrapperFromExecInitPlan(msg string) {
	ps.GeneratePlanState(ParsePlanString(msg))
}

func (ps *PlanStateWrapper) TranverseGenSTAP(fn PlanFunction) []byte {
	if ps == nil {
		return []byte{}
	}
	left := ps.LeftTree.TranverseGenSTAP(fn)
	right := ps.RightTree.TranverseGenSTAP(fn)
	local := fn(ps).([]byte)
	result := append(local, left...)
	result = append(result, right...)
	return result
}
func (ps *PlanStateWrapper) GenExecProcNodeScript() ([]byte, error) {
	if ps == nil {
		return []byte{}, nil
	}
	if ps.allInstrReady {
		return []byte{}, nil
	}

	ex, err := os.Executable()
	if err != nil {
		panic(err)
	}
	exPath := path.Dir(ex)
	filepath := path.Join(exPath, "exec_proc_node.template")
	log.Println(filepath)
	bfile, err := ioutil.ReadFile(filepath)
	if err != nil {
		return []byte{}, err
	}
	codelines := ps.TranverseGenSTAP(PrintInstrument)
	replaceaddr := bytes.Replace(bfile, []byte("PLACEHOLDER_ADDR"), codelines, -1)
	planlines := ps.TranverseGenSTAP(PrintPlan)
	//All instrument address is found
	if len(planlines) == 0 {
		ps.allInstrReady = true
	}
	replaceall := bytes.Replace(replaceaddr, []byte("PLACEHOLDER_PLAN"), planlines, -1)
	if len(codelines) > 0 || len(planlines) > 0 {
		fmt.Println(string(replaceall))
		return replaceall, nil
	}
	return []byte{}, nil
}

// UpdateInfo update Plannode info according a string map
func (ps *PlanStateWrapper) UpdateInfo(info map[string]string) {
	for key, val := range info {
		switch key {
		case "tuplecount":
			ps.TupleCount, _ = ConvertHexToFloat64(val[2:])
		case "instrument":
			ps.Instrument, _ = strconv.ParseUint(val, 0, 64)
		}
	}
}

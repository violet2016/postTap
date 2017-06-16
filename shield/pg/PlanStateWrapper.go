package pg

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"log"
	"strconv"
)

// NodeStore: node address information
type NodeStore struct {
	Address   uint64
	leftAddr  uint64
	rightAddr uint64
}
type PlanStateWrapper struct {
	PlanNodeType       int                 `json:"type id"`
	NodeTypeString     string              `json:"Node Type"`
	ParentRelationship string              `json:"Parent Relationship,omitempty"`
	Childrens          []*PlanStateWrapper `json:"Plans,omitempty"`
	//	LeftTree           *PlanStateWrapper `json:"left"`
	//	RightTree          *PlanStateWrapper `json:"right"`
	Plan       *NodeStore `json:"-"`
	Instrument uint64     `json:"-"`
	PlanRows   float64    `json:"Plan Rows"`
	TupleCount float64    `json:"Tuple Count"`
	Startup    float64
	TotalTime  float64
	NTuples    float64 `json:"Total Tuple Count"`
	NLoops     float64
	scriptGen  bool
}

type PlanFunction func(*PlanStateWrapper) interface{}

func PrintInstrument(ps *PlanStateWrapper) interface{} {
	if ps.Plan.Address == 0 || ps.Instrument == 0 {
		return []byte{}
	}
	codeline := []byte(fmt.Sprintf("\tprintdln(\"|\", pid(), \"GetInstrument\", parse_instrument(%d, %d))\n", ps.Plan.Address, ps.Instrument))
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
		case "instrument":
			ps.Instrument, err = strconv.ParseUint(val, 0, 64)
		}
		if err != nil {
			log.Fatal(err)
		}
	}
	if ps.PlanNodeType != 0 {
		ps.NodeTypeString = GetNodeTypeString(ps.PlanNodeType)
	}

	ps.Plan = pnodeStore
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
	if len(ps.Childrens) == 0 && ps.IsLeftChild(node) {
		node.ParentRelationship = "Outer"
		ps.Childrens = append(ps.Childrens, node)
		return true
	}
	if len(ps.Childrens) == 1 && ps.IsRightChild(node) {
		node.ParentRelationship = "Inner"
		ps.Childrens = append(ps.Childrens, node)
		return true
	}
	if len(ps.Childrens) == 1 && ps.Childrens[0].InsertNewNode(node) {
		return true
	}
	if len(ps.Childrens) == 2 && ps.Childrens[1].InsertNewNode(node) {
		return true
	}
	return false
}

func (ps *PlanStateWrapper) FindNodeByAddr(addr uint64) *PlanStateWrapper {
	if ps == nil || addr == 0 {
		return nil
	}
	if ps.Plan.Address == addr {
		return ps
	}
	if len(ps.Childrens) > 0 {
		if leftRes := ps.Childrens[0].FindNodeByAddr(addr); leftRes != nil {
			return leftRes
		} else if len(ps.Childrens) == 2 {
			return ps.Childrens[1].FindNodeByAddr(addr)
		}
	}
	return nil
}

func (ps *PlanStateWrapper) InitPlanStateWrapperFromExecInitPlan(msg string) {
	ps.GeneratePlanState(ParsePlanString(msg))
}

func (ps *PlanStateWrapper) TranverseGenSTAP(fn PlanFunction) []byte {
	if ps == nil {
		return []byte{}
	}
	result := fn(ps).([]byte)
	for _, child := range ps.Childrens {
		result = append(result, child.TranverseGenSTAP(fn)...)
	}
	return result
}
func (ps *PlanStateWrapper) GenExecProcNodeScript(template string) ([]byte, error) {
	if ps == nil || ps.scriptGen {
		return []byte{}, nil
	}
	bfile, err := ioutil.ReadFile(template)
	if err != nil {
		return []byte{}, err
	}

	codelines := ps.TranverseGenSTAP(PrintInstrument)

	replaceall := bytes.Replace(bfile, []byte("PLACEHOLDER_ADDR"), codelines, -1)
	//All instrument address is found

	if len(codelines) > 0 {
		ps.scriptGen = true
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
		case "startup":
			ps.Startup, _ = ConvertHexToFloat64(val[2:])
		case "total":
			ps.TotalTime, _ = ConvertHexToFloat64(val[2:])
		case "ntuples":
			ps.NTuples, _ = ConvertHexToFloat64(val[2:])
		case "nloops":
			ps.NLoops, _ = ConvertHexToFloat64(val[2:])
		case "instrument":
			ps.Instrument, _ = strconv.ParseUint(val, 0, 64)
		}
	}
}

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
	Plan          *NodeStore `json:"-"`
	Instrument    uint64     `json:"-"`
	PlanRows      float64    `json:"Plan Rows"`
	TupleCount    float64    `json:"Tuple Count"`
	allInstrReady bool
}

type PlanFunction func(*PlanStateWrapper) interface{}

func PrintInstrument(ps *PlanStateWrapper) interface{} {
	if ps.Plan.Address == 0 || ps.Instrument == 0 {
		return []byte{}
	}
	codeline := []byte(fmt.Sprintf("\t\tprintdln(\"|\", pid(), \"GetInstrument\", parse_instrument(%d, %d))\n", ps.Plan.Address, ps.Instrument))
	return codeline
}

func PrintMap(ps *PlanStateWrapper) interface{} {
	if ps.Instrument != 0 || ps.Plan.Address == 0 {
		return []byte{}
	}
	codeline := []byte(fmt.Sprintf("\tmap_addr_wait_hit[%d] = 1\n", ps.Plan.Address))
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
	if ps == nil {
		return []byte{}, nil
	}
	if ps.allInstrReady {
		return []byte{}, nil
	}

	bfile, err := ioutil.ReadFile(template)
	if err != nil {
		return []byte{}, err
	}
	maplines := ps.TranverseGenSTAP(PrintMap)
	if len(maplines) == 0 {
		ps.allInstrReady = true
	}
	replacemap := bytes.Replace(bfile, []byte("PLACEHOLDER_MAP"), maplines, -1)
	codelines := ps.TranverseGenSTAP(PrintInstrument)
	if ps.allInstrReady {
		codelines = append(codelines, []byte("\t\tprintdln(\"|\", pid(), \"EndInstrument\")\n\t\texit()\n")...)
	}
	replaceall := bytes.Replace(replacemap, []byte("PLACEHOLDER_ADDR"), codelines, -1)
	//All instrument address is found

	if len(codelines) > 0 || len(maplines) > 0 {
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

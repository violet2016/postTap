package pg

import (
	"fmt"
	"log"
	"strconv"
)

// NodeStore: node address information
type NodeStore struct {
	Address   uint64
	leftAddr  uint64
	rightAddr uint64
}

// typedef struct BufferUsage
// {
// 	long		shared_blks_hit;	/* # of shared buffer hits */
// 	long		shared_blks_read;		/* # of shared disk blocks read */
// 	long		shared_blks_dirtied;	/* # of shared blocks dirtied */
// 	long		shared_blks_written;	/* # of shared disk blocks written */
// 	long		local_blks_hit; /* # of local buffer hits */
// 	long		local_blks_read;	/* # of local disk blocks read */
// 	long		local_blks_dirtied;		/* # of shared blocks dirtied */
// 	long		local_blks_written;		/* # of local disk blocks written */
// 	long		temp_blks_read; /* # of temp blocks read */
// 	long		temp_blks_written;		/* # of temp blocks written */
// 	instr_time	blk_read_time;	/* time spent reading */
// 	instr_time	blk_write_time; /* time spent writing */
// } BufferUsage;

type PlanStateWrapper struct {
	PlanNodeType       int                 `json:"type id"`
	NodeTypeString     string              `json:"Node Type"`
	ParentRelationship string              `json:"Parent Relationship,omitempty"`
	Childrens          []*PlanStateWrapper `json:"Plans,omitempty"`
	Plan               *NodeStore          `json:"-"`
	Instrument         uint64              `json:"-"`
	PlanRows           float64             `json:"Plan Rows"`
	PlanWidth          int                 `json:"Plan Width"`
	TupleCount         float64             `json:"Tuple Count"`
	Running            bool                `json:"Running"`
	StartupCost        float64             `json:"StartupCost"`
	TotalCost          float64             `json:"TotalCost"`
	// Accumulated
	Startup   float64 `json:"Startup Time,omitempty"`
	TotalTime float64 `json:"Total Time,omitempty"`
	NTuples   float64 `json:"Actual Rows,omitempty"`
	NLoops    float64 `json:"Actual Loops,omitempty"`
	// Buffer
	SharedHitBlocks     uint64 `json:"Shared Hit Blocks,omitempty"`
	SharedReadBlocks    uint64 `json:"Shared Read Blocks,omitempty"`
	SharedDirtiedBlocks uint64 `json:"Shared Dirtied Blocks,omitempty"`
	SharedWrittenBlocks uint64 `json:"Shared Written Blocks,omitempty"`
	LocalHitBlocks      uint64 `json:"Local Hit Blocks,omitempty"`
	LocalReadBlocks     uint64 `json:"Local Read Blocks,omitempty"`
	LocalDirtiedBlocks  uint64 `json:"Local Dirtied Blocks,omitempty"`
	LocalWrittenBlocks  uint64 `json:"Local Written Blocks,omitempty"`
	TempReadBlocks      uint64 `json:"Temp Read Blocks,omitempty"`
	TempWrittenBlocks   uint64 `json:"Temp Written Blocks,omitempty"`
	//	IOReadTime          uint64              `json:"I/O Read Time,omitempty"`
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
		case "startup_cost":
			ps.StartupCost, err = ConvertHexToFloat64(val[2:])
		case "total_cost":
			ps.TotalCost, err = ConvertHexToFloat64(val[2:])
		case "plan_width":
			ps.PlanWidth, err = strconv.Atoi(val)
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

// UpdateInfo update Plannode info according a string map
func (ps *PlanStateWrapper) UpdateInfo(info map[string]string) {
	for key, val := range info {
		switch key {
		case "tuplecount":
			ps.TupleCount, _ = ConvertHexToFloat64(val[2:])
		case "running":
			ps.Running, _ = strconv.ParseBool(val[2:])
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

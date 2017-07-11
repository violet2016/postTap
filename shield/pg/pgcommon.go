package pg

// TODO generate this file automatically?
// PlanState enum
const (
	T_Invalid = iota

	/*
	 * TAGS FOR EXECUTOR NODES (execnodes.h)
	 */
	T_IndexInfo
	T_ExprContext
	T_ProjectionInfo
	T_JunkFilter
	T_ResultRelInfo
	T_EState
	T_TupleTableSlot

	/*
	 * TAGS FOR PLAN NODES (plannodes.h)
	 */
	T_Plan
	T_Result
	T_ProjectSet
	T_ModifyTable
	T_Append
	T_MergeAppend
	T_RecursiveUnion
	T_BitmapAnd
	T_BitmapOr
	T_Scan
	T_SeqScan
	T_SampleScan
	T_IndexScan
	T_IndexOnlyScan
	T_BitmapIndexScan
	T_BitmapHeapScan
	T_TidScan
	T_SubqueryScan
	T_FunctionScan
	T_ValuesScan
	T_TableFuncScan
	T_CteScan
	T_NamedTuplestoreScan
	T_WorkTableScan
	T_ForeignScan
	T_CustomScan
	T_Join
	T_NestLoop
	T_MergeJoin
	T_HashJoin
	T_Material
	T_Sort
	T_Group
	T_Agg
	T_WindowAgg
	T_Unique
	T_Gather
	T_GatherMerge
	T_Hash
	T_SetOp
	T_LockRows
	T_Limit
	/* these aren't subclasses of Plan: */
	T_NestLoopParam
	T_PlanRowMark
	T_PlanInvalItem

	/*
	 * TAGS FOR PLAN STATE NODES (execnodes.h)
	 *
	 * These should correspond one-to-one with Plan node types.
	 */
	T_PlanState
	T_ResultState
	T_ProjectSetState
	T_ModifyTableState
	T_AppendState
	T_MergeAppendState
	T_RecursiveUnionState
	T_BitmapAndState
	T_BitmapOrState
	T_ScanState
	T_SeqScanState
	T_SampleScanState
	T_IndexScanState
	T_IndexOnlyScanState
	T_BitmapIndexScanState
	T_BitmapHeapScanState
	T_TidScanState
	T_SubqueryScanState
	T_FunctionScanState
	T_TableFuncScanState
	T_ValuesScanState
	T_CteScanState
	T_NamedTuplestoreScanState
	T_WorkTableScanState
	T_ForeignScanState
	T_CustomScanState
	T_JoinState
	T_NestLoopState
	T_MergeJoinState
	T_HashJoinState
	T_MaterialState
	T_SortState
	T_GroupState
	T_AggState
	T_WindowAggState
	T_UniqueState
	T_GatherState
	T_GatherMergeState
	T_HashState
	T_SetOpState
	T_LockRowsState
	T_LimitState
)

// PlanStateString is the map of enum and print strings
var planStateStringMap = map[int]string{
	T_ResultState:          "Result",
	T_SeqScanState:         "Seq Scan",
	T_LimitState:           "Limit",
	T_AggState:             "Aggregate",
	T_SortState:            "Sort",
	T_GroupState:           "Group by",
	T_NestLoopState:        "Nested Loop",
	T_MaterialState:        "Materialize",
	T_FunctionScanState:    "Function Scan",
	T_BitmapHeapScanState:  "Bitmap Heap Scan",
	T_BitmapIndexScanState: "Bitmap Index Scan",
	T_SubqueryScanState:    "Subquery Scan",
	T_HashJoinState:        "Hash Join",
	T_MergeJoinState:       "Merge Join",
	T_JoinState:            "Join",
	T_HashState:            "Hash",
	T_UniqueState:          "Unique",
	T_CteScanState:         "CTE Scan",
	T_WindowAggState:       "WindowAgg",
	T_GatherState:          "Gather",
	T_GatherMergeState:     "Gather Merge",
}

func GetNodeTypeString(typeCode int) string {
	return planStateStringMap[typeCode]
}

type InstrAttr struct {
	MemberType string
	Offset     int
}

var InstrumentMember = map[string]map[string]InstrAttr{
	"base":        map[string]InstrAttr{"tuplecount": InstrAttr{"long", 48}, "running": InstrAttr{"int8", 2}},
	"accumulated": map[string]InstrAttr{"startup": InstrAttr{"long", 168}, "total": InstrAttr{"long", 176}, "ntuples": InstrAttr{"long", 184}, "nloops": InstrAttr{"long", 192}},
	"buffer":      map[string]InstrAttr{},
}

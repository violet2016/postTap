package pg

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
)

// PlanStateString is the map of enum and print strings
var planStateStringMap = map[int]string{
	T_Result:   "Result",
	T_SeqScan:  "Seq Scan",
	T_Limit:    "Limit",
	T_Agg:      "Aggregate",
	T_NestLoop: "Nested Loop",
	T_Material: "Materialize",
}

func GetNodeTypeString(typeCode int) string {
	return planStateStringMap[typeCode]
}

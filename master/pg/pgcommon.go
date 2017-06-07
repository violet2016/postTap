package pg

// PlanState enum
const (
	PlanState   = 200 + iota
	ResultState //201
	ModifyTableState
	AppendState
	MergeAppendState
	RecursiveUnionState
	BitmapAndState
	BitmapOrState
	ScanState
	SeqScanState //209
	SampleScanState
	IndexScanState
	IndexOnlyScanState
	BitmapIndexScanState
	BitmapHeapScanState
	TidScanState
	SubqueryScanState
	FunctionScanState
	ValuesScanState
	CteScanState
	WorkTableScanState
	ForeignScanState
	CustomScanState
	JoinState
	NestLoopState
	MergeJoinState
	HashJoinState
	MaterialState
	SortState
	GroupState
	AggState
	WindowAggState
	UniqueState
	GatherState
	HashState
	SetOpState
	LockRowsState
	LimitState
)

// PlanStateString is the map of enum and print strings
var PlanStateString = map[int]string{
	ResultState:  "Result",
	SeqScanState: "Seq Scan",
	LimitState:   "Limit",
}

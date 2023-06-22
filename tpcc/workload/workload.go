package workload

import (
	"context"
	"database/sql"
)

// Workloader is the interface for running customized workload
type Workloader interface {
	Name() string
	InitContext(ctx context.Context, threadID int, seed int64) context.Context
	InitWorkloader(ctx context.Context) context.Context
	PrepareTransactions(ctx context.Context, transactionsList []interface{}) []interface{}
	SetDBConnections(dbconns []*sql.DB)
	SetDB(db *sql.DB)
	//GetDB() *sql.DB
	CleanupThread(ctx context.Context, threadID int)
	//Prepare(ctx context.Context, threadID int) error
	//CheckPrepare(ctx context.Context, threadID int) error
	Run(ctx context.Context, threadID int, txnCounter int) error
	//Cleanup(ctx context.Context, threadID int) error
	//Check(ctx context.Context, threadID int) error
	OutputStats(ifSummaryReport bool)
	//DBName() string

	IsPlanReplayerDumpEnabled() bool
	PreparePlanReplayerDump() error
	FinishPlanReplayerDump() error
	//Exec(sql string) error
}

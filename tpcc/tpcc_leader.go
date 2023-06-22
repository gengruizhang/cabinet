package tpcc

import (
	"cabinet/tpcc/workload"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"time"
)

type TpccArgs struct {
	TpccConfig   Config
	Transactions map[int][]interface{}
	Seeds        map[int]int64
}

var globalCtx1 context.Context

//type txnsPerThread struct {
//	thread           int
//	transactionsList []interface{}
//}

func PrepareArgs(tpccConfig Config, txnNum int) (map[int][]interface{}, map[int]int64, error) {

	var (
		w   workload.Workloader
		err error
	)

	w, err = NewWorkloader(&tpccConfig)

	if err != nil {
		fmt.Printf("Failed to init work loader in the TPCC leader: %v\n", err)
		os.Exit(1)
	}

	//number of transactions for each thread
	count := txnNum / tpccConfig.Threads

	//Create the execution context for each follower
	//ctx := w.InitWorkloader(context.Background())

	//var transactions txnsPerThread
	txnsPerThread := make(map[int][]interface{})
	//contexts := make(map[int]context.Context)
	//ctxValues := make(map[int]*TpccState)
	//randomValues := make(map[int]*rand.Rand)
	seeds := make(map[int]int64)

	for j := 0; j < tpccConfig.Threads; j++ {
		transactionsList := []interface{}{}
		seed := time.Now().UnixNano()
		ctx := w.InitContext(context.Background(), j, seed)
		//r := getTPCCState(ctx).TpcState.R
		for i := 0; i < count || count <= 0; i++ {
			transactionsList = w.PrepareTransactions(ctx, transactionsList)
			if err != nil {
				if !tpccConfig.Silence {
					fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), tpccConfig.Action, err)
				}
				if !tpccConfig.IgnoreError {
					return txnsPerThread, seeds, err
				}
			}
		}
		//contexts[j] = ctx
		//ctxValues[j] = ctx.Value(stateKey).(*TpccState)
		//randomValues[j] = r
		seeds[j] = seed
		txnsPerThread[j] = transactionsList
	}

	gob.Register(NewOrderTxn{})
	gob.Register(PaymentTxn{})
	gob.Register(StockLevelTxn{})
	gob.Register(OrderStatusTxn{})
	gob.Register(DeliveryTxn{})
	gob.Register(TpccState{})

	return txnsPerThread, seeds, nil

}

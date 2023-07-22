package tpcc

import (
	"cabinet/tpcc/workload"
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"os"
	"runtime"
	"sync"
	"time"
)

func executeTpcc(action string, connections []*sql.DB) {

	if TpccConfig.PprofAddr != "" {
		go func() {
			if err := http.ListenAndServe(TpccConfig.PprofAddr, http.DefaultServeMux); err != nil {
				fmt.Printf("Failed to listen pprofAddr: %v\n", err)
				os.Exit(1)
			}
		}()
	}

	//TODO check for tpccConfig.MetricsAddr != ""

	runtime.GOMAXPROCS(TpccConfig.MaxProcs)
	//defer closeConnections(connections)

	var (
		w   workload.Workloader
		err error
	)

	w, err = NewWorkloader(&TpccConfig)
	w.SetDBConnections(connections)
	//w.SetDB(clientDB)
	if err != nil {
		fmt.Printf("Failed to init work loader: %v\n", err)
		os.Exit(1)
	}

	timeoutCtx, cancel := context.WithTimeout(context.Background(), TpccConfig.TotalTime)
	defer cancel()

	executeWorkload(timeoutCtx, w, TpccConfig.Threads, action)
	fmt.Println("Finished")
	w.OutputStats(true)

}

func executeWorkload(ctx context.Context, w workload.Workloader, threads int, action string) {

	var wg sync.WaitGroup
	wg.Add(threads)

	outputCtx, outputCancel := context.WithCancel(ctx)
	ch := make(chan struct{}, 1)
	go func() {
		ticker := time.NewTicker(TpccConfig.OutputInterval)
		defer ticker.Stop()

		for {
			select {
			case <-outputCtx.Done():
				ch <- struct{}{}
				return
			case <-ticker.C:
				w.OutputStats(false)
			}
		}
	}()

	for i := 0; i < threads; i++ {
		go func(index int) {
			defer wg.Done()
			if err := execute(ctx, w, action, threads, index); err != nil {
				if action == "prepare" {
					panic(fmt.Sprintf("a fatal occurred when preparing data: %v", err))
				}
				fmt.Printf("execute %s failed, err %v\n", action, err)
				return
			}
		}(i)
	}

	wg.Wait()

	outputCancel()

}

func execute(timeoutCtx context.Context, w workload.Workloader, action string, threads, index int) error {

	//count := TpccConfig.TotalCount / threads

	count := len(Transactions[index])

	ctx := w.InitContext(context.Background(), index, Seeds[index])
	defer w.CleanupThread(ctx, index)

	//switch action {
	//case "prepare":
	//	// Do cleanup only if dropData is set and not generate csv data.
	//	if tpccConfig.DropData {
	//		if err := w.Cleanup(ctx, index); err != nil {
	//			return err
	//		}
	//	}
	//	return w.Prepare(ctx, index)
	//case "cleanup":
	//	return w.Cleanup(ctx, index)
	//case "check":
	//	return w.Check(ctx, index)

	for i := 0; i < count || count <= 0; i++ {
		err := w.Run(ctx, index, i)
		if err != nil {
			if !TpccConfig.Silence {
				fmt.Printf("[%s] execute %s failed, err %v\n", time.Now().Format("2006-01-02 15:04:05"), action, err)
			}
			if !TpccConfig.IgnoreError {
				return err
			}
		}
		select {
		case <-timeoutCtx.Done():
			return nil
		default:
		}
	}

	return nil

}

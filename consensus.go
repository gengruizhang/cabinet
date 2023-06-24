package main

import (
	"cabinet/eval"
	"cabinet/mongodb"
	"cabinet/tpcc"
	"math"
	"strconv"
	"time"
)

func startSyncCabInstance() {
	leaderPrioClock := 0
	//pm := cabservice.NewPrioMgr(1, 1)
	mongoDBQueries, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "run_workload" + mongoLoadType + ".dat")
	if err != nil {
		log.Errorf("ReadQueryFromFile failed | err: %v", err)
		return
	}

	executionRounds := int(math.Floor(float64(tpcc.TpccConfig.TotalCount) / float64(batchsize)))
	allArgs, err := registerTPCCTxns(executionRounds)
	if err != nil {
		log.Errorf("error during transactions preparation | err: %v", err)
	}

	for {

		serviceMethod := "CabService.ConsensusService"

		receiver := make(chan ReplyInfo, numOfServers)

		startTime := time.Now()

		// 1. get priority
		fpriorities := pManager.GetFollowerPriorities(leaderPrioClock)
		log.Infof("pClock: %v | priorities: %+v", leaderPrioClock, fpriorities)

		// 2. broadcast rpcs
		switch evalType {
		case PlainMsg:
			issuePlainMsgOps(leaderPrioClock, fpriorities, serviceMethod, receiver)
		case TPCC:
			perfM.RecordStarter(leaderPrioClock)
			//transactions, seeds, err := tpcc.PrepareArgs(tpcc.TpccConfig)
			//if err != nil {
			//	log.Errorf("error during transactions preparation | err: %v", err)
			//}
			//args := tpcc.TpccArgs{
			//	TpccConfig:   tpcc.TpccConfig,
			//	Transactions: transactions,
			//	Seeds:        seeds,
			//}
			if issueTPCCOps(leaderPrioClock, fpriorities, serviceMethod, receiver, allArgs) {
				err := perfM.SaveToFileTpcc()
				if err != nil {
					log.Errorf("perfM save to file failed | err: %v", err)
				}
				return
			}
		case MongoDB:
			perfM.RecordStarter(leaderPrioClock)

			if issueMongoDBOps(leaderPrioClock, fpriorities, serviceMethod, receiver, mongoDBQueries) {
				err := perfM.SaveToFile()
				if err != nil {
					log.Errorf("perfM save to file failed | err: %v", err)
				}
				return
			}
		}

		// 3. waiting for results
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)
		var followersResults []ReplyInfo

		for rinfo := range receiver {

			prioQueue <- rinfo.SID
			log.Infof("recv pClock: %v | serverID: %v", leaderPrioClock, rinfo.SID)

			fpriorities := pManager.GetFollowerPriorities(leaderPrioClock)

			prioSum += fpriorities[rinfo.SID]

			followersResults = append(followersResults, rinfo)

			if prioSum > mypriority.Majority {
				err := perfM.RecordFinisher(leaderPrioClock)
				if err != nil {
					log.Errorf("PerfMeter failed | err: %v", err)
					return
				}

				//If we run TPCC, keep the execution metrics
				if len(rinfo.Recv.TpccMetrics) != 0 {
					RecordTpccMetrics(&perfM, rinfo, leaderPrioClock, followersResults)
				}

				timeElapsed := time.Now().Sub(startTime)
				mystate.AddCommitIndex(batchsize)

				log.Infof("consensus reached | insID: %v | total time elapsed: %v | cmtIndex: %v",
					leaderPrioClock, timeElapsed.String(), mystate.GetCommitIndex())
				break
			}
		}

		leaderPrioClock++
		err := pManager.UpdateFollowerPriorities(leaderPrioClock, prioQueue, mystate.GetLeaderID())
		if err != nil {
			log.Errorf("UpdateFollowerPriorities failed | err: %v", err)
		}
		log.Infof("prio updated for pClock %v", leaderPrioClock)
	}
}

func issuePlainMsgOps(pClock prioClock, p map[serverID]priority, method string, r chan ReplyInfo) {
	conns.RLock()
	defer conns.RUnlock()

	var plainMessage [][]byte
	for i := 0; i < batchsize; i++ {
		plainMessage = append(plainMessage, genRandomBytes(msgsize))
	}

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			Type:      PlainMsg,
			CmdPlain:  plainMessage,
		}

		go executeRPC(conn, method, args, r)
	}
}

func issueTPCCOps(pClock prioClock, p map[serverID]priority, method string, r chan ReplyInfo, allArgs []tpcc.TpccArgs) (allDone bool) {
	conns.RLock()
	defer conns.RUnlock()

	if pClock >= len(allArgs) {
		log.Infof("TPCC evaluation finished")
		allDone = true
		return
	}

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			Type:      TPCC,
			CmdTPCC:   allArgs[pClock],
		}

		go executeRPC(conn, method, args, r)

	}

	return
}

func registerTPCCTxns(executionRounds int) (allArgs []tpcc.TpccArgs, err error) {

	var transactions map[int][]interface{}
	var seeds map[int]int64

	for i := 0; i < executionRounds; i++ {
		if i == executionRounds-1 {
			txnLeft := tpcc.TpccConfig.TotalCount - i*batchsize
			transactions, seeds, err = tpcc.PrepareArgs(tpcc.TpccConfig, txnLeft)
		} else {
			transactions, seeds, err = tpcc.PrepareArgs(tpcc.TpccConfig, batchsize)
		}

		if err != nil {
			log.Errorf("error during transactions preparation | err: %v", err)
		}
		args := tpcc.TpccArgs{
			TpccConfig:   tpcc.TpccConfig,
			Transactions: transactions,
			Seeds:        seeds,
		}
		allArgs = append(allArgs, args)
	}

	return allArgs, err

}

func RecordTpccMetrics(m *eval.PerfMeter, lastReply ReplyInfo, pClock prioClock, followersResults []ReplyInfo) {

	for j := 0; j < 5; j++ {

		//txnMetric -> transaction metrics of last response
		txnMetric := lastReply.Recv.TpccMetrics[j]
		avgTPM := 0.0
		avgExecLat := 0.0
		for _, res := range followersResults {
			tpm, _ := strconv.ParseFloat(res.Recv.TpccMetrics[j]["TPM"], 64)
			lat, _ := strconv.ParseFloat(res.Recv.TpccMetrics[j]["Avg(ms)"], 64)
			avgTPM += tpm
			avgExecLat += lat
		}
		avgTPM = avgTPM / float64(len(followersResults))
		avgExecLat = avgExecLat / float64(len(followersResults))
		m.RecordTpccTxnMetrics(pClock, txnMetric, avgTPM, avgExecLat)
	}

}

func issueMongoDBOps(pClock prioClock, p map[serverID]priority, method string, r chan ReplyInfo, allQueries []mongodb.Query) (allDone bool) {
	conns.RLock()
	defer conns.RUnlock()

	left := pClock * batchsize
	right := (pClock+1)*batchsize - 1
	if right > len(allQueries) {
		log.Infof("MongoDB evaluation finished")
		allDone = true
		return
	}

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			Type:      MongoDB,
			CmdMongo:  allQueries[left:right],
		}

		go executeRPC(conn, method, args, r)
	}

	return
}

func executeRPC(conn *ServerDock, serviceMethod string, args *Args, receiver chan ReplyInfo) {
	reply := Reply{}

	stack := make(chan struct{}, 1)

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] = stack
	conn.jobQMu.Unlock()

	if args.PrioClock > 0 {
		// Waiting for the completion of its previous RPC
		<-conn.jobQ[args.PrioClock-1]
	}

	err := conn.txClient.Call(serviceMethod, args, &reply)

	if err != nil {
		log.Errorf("RPC call error: %v", err)
		return
	}

	rinfo := ReplyInfo{
		SID:    conn.serverID,
		PClock: args.PrioClock,
		Recv:   reply,
	}
	receiver <- rinfo

	conn.jobQMu.Lock()
	conn.jobQ[args.PrioClock] <- struct{}{}
	conn.jobQMu.Unlock()

	log.Debugf("RPC %s succeeded | result: %+v", serviceMethod, rinfo)
}

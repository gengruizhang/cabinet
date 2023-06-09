package main

import (
	"cabinet/mongodb"
	"time"
)

func startSyncCabInstance() {
	leaderPrioClock := 0
	//pm := cabservice.NewPrioMgr(1, 1)
	mongoDBAllQueries, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "run_workload" + mongoLoadType + ".dat")
	if err != nil {
		log.Errorf("ReadQueryFromFile failed | err: %v", err)
		return
	}

	for {

		serviceMethod := "CabService.ConsensusService"

		receiver := make(chan ReplyInfo, numOfServers)

		startTime := time.Now()

		// 1. get priority
		fpriorities := pManager.GetFollowerPriorities(leaderPrioClock)
		log.Infof("pClock: %v | priorities: %+v", leaderPrioClock, fpriorities)

		// 2. broadcast rpcs

		// mongoDB
		switch evalType {
		case PlainMsg:
			issuePlainMsgOps(leaderPrioClock, fpriorities, serviceMethod, receiver)
		case TPCC:

		case MongoDB:
			left := leaderPrioClock * batchsize
			right := (leaderPrioClock+1)*batchsize - 1
			if right > len(mongoDBAllQueries) {
				log.Infof("MongoDB evaluation finished")
				break
			}
			mongoCmd := mongoDBAllQueries[left:right]
			issueMongoDBOps(leaderPrioClock, fpriorities, serviceMethod, receiver, mongoCmd)
		}

		// 3. waiting for results
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)

		for rinfo := range receiver {

			prioQueue <- rinfo.SID
			log.Infof("recv pClock: %v | serverID: %v", leaderPrioClock, rinfo.SID)

			fpriorities := pManager.GetFollowerPriorities(leaderPrioClock)

			prioSum += fpriorities[rinfo.SID]

			if prioSum > mypriority.Majority {
				timeElapsed := time.Now().Sub(startTime)
				mystate.AddCommitIndex(batchsize)

				log.Infof("consensus reached | insID: %v | total time elapsed: %v | cmtIndex: %v",
					leaderPrioClock, timeElapsed.String(), mystate.GetCommitIndex())
				break
			}
		}

		leaderPrioClock++
		pManager.UpdateFollowerPriorities(leaderPrioClock, prioQueue)
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

func issueMongoDBOps(pClock prioClock, p map[serverID]priority, method string, r chan ReplyInfo, cmd []mongodb.Query) {
	conns.RLock()
	defer conns.RUnlock()

	//gob.Register([]mongodb.Query{})
	// cmd is all queries

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			Type:      MongoDB,
			CmdMongo:  cmd,
		}

		go executeRPC(conn, method, args, r)
	}

}

func issueTPCCOps() {

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

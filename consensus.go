package main

import (
	"cabinet/mongodb"
	"time"
)

func startSyncCabInstance() {
	leaderPrioClock := 0
	//pm := cabservice.NewPrioMgr(1, 1)
	for {

		serviceMethod := "CabService.ConsensusService"

		receiver := make(chan Reply, numOfServers)

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
			issueMongoDBOps(leaderPrioClock, fpriorities, serviceMethod, receiver)
		}

		// 3. waiting for results
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)

		for reply := range receiver {

			if reply.PrioClock < leaderPrioClock {
				log.Debugf("stale pClock | remote pClock: %v | current pClock: %v", reply.PrioClock, leaderPrioClock)
				continue
			}

			prioQueue <- reply.ServerID
			log.Infof("recv pClock: %v | serverID: %v", leaderPrioClock, reply.ServerID)

			fpriorities := pManager.GetFollowerPriorities(leaderPrioClock)

			prioSum += fpriorities[reply.ServerID]

			if prioSum > mypriority.Majority {
				timeElapsed := time.Now().Sub(startTime)
				log.Infof("consensus reached | insID: %v | time elapsed: %v", leaderPrioClock, timeElapsed.String())
				break
			}
		}

		leaderPrioClock++
		pManager.UpdateFollowerPriorities(leaderPrioClock, prioQueue)
		log.Infof("prio updated for pClock %v", leaderPrioClock)
	}
}

func issuePlainMsgOps(pClock prioClock, p map[serverID]priority, method string, r chan Reply) {
	conns.RLock()
	defer conns.RUnlock()

	for _, conn := range conns.m {
		args := &Args{
			PrioClock: pClock,
			PrioVal:   p[conn.serverID],
			Type:      PlainMsg,
			CmdPlain:  genRandomBytes(1024),
		}

		go executeRPC(conn, method, args, r)
	}
}

func issueMongoDBOps(pClock prioClock, p map[serverID]priority, method string, r chan Reply) {
	conns.RLock()
	defer conns.RUnlock()

	//gob.Register([]mongodb.Query{})
	cmd, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "run_workload" + mongoLoadType + ".dat")
	if err != nil {
		log.Errorf("ReadQueryFromFile failed | err: %v", err)
		return
	}

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

func executeRPC(conn ServerDock, serviceMethod string, args *Args, receiver chan Reply) {
	reply := Reply{}

	err := conn.txClient.Call(serviceMethod, args, &reply)

	if err != nil {
		log.Errorf("RPC call error: %v", err)
		return
	}

	log.Debugf("RPC %s succeeded | result: %v", serviceMethod, reply)

	receiver <- reply
}

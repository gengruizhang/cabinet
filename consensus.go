package main

import (
	"cabinet/mongodb"
	"encoding/gob"
	"time"
)

var leaderPrioClock = 0

func startSyncCabInstance() {
	//pm := cabservice.NewPrioMgr(1, 1)
	for {

		serviceMethod := "CabService.ConsensusService"

		receiver := make(chan Reply, numOfServers)

		startTime := time.Now()

		// 1. get priority
		pManager.RLock()
		fpriorities := pManager.m[leaderPrioClock]
		pManager.RUnlock()

		// 2. broadcast rpcs

		conns.RLock()

		// mongoDB
		gob.Register([]mongodb.Query{})
		cmd, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "run_workload" + loadType + ".dat")
		if err != nil {
			log.Errorf(err.Error())
		}

		for _, conn := range conns.m {
			args := &Args{
				PrioClock: leaderPrioClock,
				PrioVal:   fpriorities[conn.serverID],
				Type:      MongoDB,
				Cmd:       cmd,
			}

			go executeRPC(conn, serviceMethod, args, receiver)
		}

		// plain msg
		// for _, conn := range conns.m {
		// 	args := &Args{
		// 		PrioClock: leaderPrioClock,
		// 		PrioVal:   fpriorities[conn.serverID],
		// 		Type:      PlainMsg,
		// 		Cmd:       []string{"./cabservice/sum.py", "100_000_000"},
		// 	}

		// 	go executeRPC(conn, serviceMethod, args, receiver)
		// }

		conns.RUnlock()

		// 3. waiting for results
		prioSum := mypriority.PrioVal
		prioQueue := make(chan serverID, numOfServers)

		for reply := range receiver {

			if reply.PrioClock < leaderPrioClock {
				log.Debugf("stale pClock | remote pClock: %v | current pClock: %v", reply.PrioClock, leaderPrioClock)
				continue
			}

			prioQueue <- reply.ServerID

			pManager.RLock()
			fpriorities := pManager.m[leaderPrioClock]
			pManager.RUnlock()

			prioSum += fpriorities[reply.ServerID]

			if prioSum > mypriority.Majority {
				timeElapsed := time.Now().Sub(startTime)
				log.Infof("consensus reached | insID: %v | time elapsed: %v", leaderPrioClock, timeElapsed.String())
				break
			}
		}

		leaderPrioClock++
		updateFollowerPriorities(leaderPrioClock, prioQueue)
		log.Infof("prio updated for pClock %v", leaderPrioClock)
	}
}

func executeRPC(conn ServerDock, serviceMethod string, args *Args, receiver chan Reply) {
	reply := Reply{}

	err := conn.txClient.Call(serviceMethod, args, &reply)

	if err != nil {
		log.Errorf("RPC call error: %v", err)
		return
	}

	log.Infof("RPC %s succeeded | result: %v", serviceMethod, reply)

	receiver <- reply
}

package main

import (
	"time"
)

var insCounter = 0

func startSyncCabInstance() {
	insCounter++
	//pm := cabservice.NewPrioMgr(1, 1)
	serviceMethod := "CabService.ExecutePython"

	args := &Args{
		PrioClock: 10,
		Prio:      20,
		Cmd:       []string{"./cabservice/sum.py", "100_000_000"},
	}

	receiver := make(chan Reply, numOfServers)

	startTime := time.Now()

	for k, _ := range conns {
		go func(conn ServerDock, k int) {
			reply := Reply{}

			//if err := conn.prioClient.Call("CabService.Add", args, &reply); err == nil {
			//	log.Infof("prioClient call succeeded | result: %v", reply)
			//}
			go func() {
				err := conn.txClient.Call(serviceMethod, args, &reply)
				if err != nil {
					log.Errorf("RPC call error: %v", err)
					return
				}
			}()

			go func() {
				time.Sleep(2 * time.Second)
				log.Infof("doing this ...")
				err := conn.txClient.Call("CabService.Add", args, &reply)

				if err != nil {
					log.Errorf("RPC call error: %v", err)
					return
				}
				log.Infof("CabService.Add succeeded | result: %v", reply)
			}()

			log.Debugf("call succeeded | %v", conn.addr)
			receiver <- reply
		}(conns[k], k)
	}

	prioSum := leaderPrio
	for reply := range receiver {
		prioSum += reply.Prio
		if prioSum > prioThreshold {
			timeElapsed := time.Now().Sub(startTime)
			log.Infof("consensus reached | insID: %v | time elapsed: %v", insCounter, timeElapsed.String())
			return
		}
	}
}

package main

import (
	"cabinet/smr"
	"fmt"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()
var mypriority = smr.NewServerPriority(-1, 0)
var mystate = smr.NewServerState()
var pscheme []priority

type serverID = int
type prioClock = int
type priority = float64

func init() {
	loadCommandLineInputs()
	setLogger(logLevel)

	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0)

	pscheme, mypriority.Majority = initPriorities(numOfServers, faults, 1)
}

func main() {
	fmt.Println("information board")
	fmt.Printf("priority scheme: %v\n", pscheme)
	fmt.Printf("majority: %v\n", mypriority.Majority)

	if myServerID == 0 {
		mypriority.PrioVal = pscheme[0] // leader has the highest priority
		establishRPCs()
		startSyncCabInstance()
	} else {
		runFollower()
		// tpccDependency()
		// mongoDependency()
	}
}

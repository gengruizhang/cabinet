package main

import (
	"cabinet/mongodb"
	"cabinet/smr"
	"fmt"
	"github.com/sirupsen/logrus"
)

var log = logrus.New()

var mypriority = smr.NewServerPriority(-1, 0)
var mystate = smr.NewServerState()
var pManager smr.PriorityManager

// Mongo DB variables
var mongoDbFollower *mongodb.MongoFollower

// Create Cabinet alias
type serverID = int
type prioClock = int
type priority = float64

func init() {
	loadCommandLineInputs()
	setLogger(logLevel)

	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0)

	pManager.Init(numOfServers, faults, 1)
}

func main() {
	mypriority.Majority = pManager.GetMajority()
	pscheme := pManager.GetPriorityScheme()

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
	}
}

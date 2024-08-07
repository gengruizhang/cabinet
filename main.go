package main

import (
	"cabinet/eval"
	"cabinet/mongodb"
	"cabinet/smr"
	"cabinet/tpcc"
	"fmt"

	"github.com/sirupsen/logrus"
)

var log = logrus.New()

var mypriority = smr.NewServerPriority(-1, 0)
var mystate = smr.NewServerState()
var pManager smr.PriorityManager

var perfM eval.PerfMeter

// Mongo DB variables
var mongoDbFollower *mongodb.MongoFollower

// TPCC variables
var tpccFollower *tpcc.TPCCFollower

// Create Cabinet alias
type serverID = int
type prioClock = int
type priority = float64

func init() {
	fmt.Println("program starts ...")
	loadCommandLineInputs()
	setLogger(logLevel)

	mystate.SetMyServerID(myServerID)
	mystate.SetLeaderID(0)

	pManager.Init(numOfServers, quorum, 1, ratioTryStep, enablePriority)

	fileName := fmt.Sprintf("s%d_n%d_f%d_b%d_%s", myServerID, numOfServers, quorum, batchsize, suffix)

	perfM.Init(1, batchsize, fileName)
}

func main() {
	mypriority.Majority = pManager.GetMajority()
	pscheme := pManager.GetPriorityScheme()

	fmt.Println("information board")
	fmt.Printf("priority scheme: %v\n", pscheme)
	fmt.Printf("majority: %v\n", mypriority.Majority)

	if myServerID == 0 {
		mypriority.PrioVal = pscheme[0]
		readTpccConfig() // leader has the highest priority
		log.Infof("readTpccConfig() was successful.")

		establishRPCs()
		log.Infof("establishRPCs() was successful.")

		startSyncCabInstance()

	} else {
		runFollower()
	}
}

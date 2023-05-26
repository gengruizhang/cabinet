package main

import (
	"github.com/sirupsen/logrus"
	"sync"
)

var log = logrus.New()
var myPrio PriorityMgr

type serverID = int
type prioClock = int

// 1, 2, 3, 4, 5
var priorities = struct {
	sync.Mutex
	m map[prioClock]map[serverID]int
}{m: make(map[prioClock]map[serverID]int)}

func init() {
	myPrio = NewPrioMgr(1, 4)
	loadCommandLineInputs()
	prioThreshold = 8
	leaderPrio = 5
	setLogger(logLevel)
}

func main() {
	if myServerID == 0 {
		establishRPCs()
		startSyncCabInstance()
	} else {
		runFollower()
	}
}

package main

import (
	"cabinet/config"
	"cabinet/mongodb"
	"encoding/gob"
	"net"
	"net/rpc"
	"sync"
)

// conns does not store the operating server'mystate information
var conns = struct {
	sync.RWMutex
	m map[int]ServerDock
}{
	m: make(map[int]ServerDock),
}

type ServerDock struct {
	serverID   int
	addr       string
	txClient   *rpc.Client
	prioClient *rpc.Client
}

func runFollower() {
	serverConfig := config.Parser(numOfServers, configPath)
	ipIndex := config.ServerIP
	rpcPortIndex := config.ServerRPCListenerPort

	myAddr := serverConfig[myServerID][ipIndex] + ":" + serverConfig[myServerID][rpcPortIndex]
	log.Debugf("config: serverID %d | addr: %s", myServerID, myAddr)

	err := rpc.Register(NewCabService())
	if err != nil {
		log.Fatalf("rp.Reister failed | error: %v", err)
		return
	}

	// Mongo DB follower initialization
	gob.Register([]mongodb.Query{})
	queryTable := "usertable"

	mongoDbFollower = mongodb.NewMongoFollower(clientNum, int(1))
	queriesToLoad, err := mongodb.ReadQueryFromFile(mongodb.DataPath + "workload" + loadType + ".dat")
	if err != nil {
		log.Errorf("getting load data failed | error: %v", err)
		return
	}

	if myServerID == 1 || !runLocal {
		err = mongoDbFollower.ClearTable(queryTable)
		if err != nil {
			log.Errorf("clear table failed | error: %v", err)
			return
		}
		log.Debugf("loading data to Mongo DB")
		_, _, err = mongoDbFollower.FollowerAPI(queriesToLoad)
		if err != nil {
			log.Errorf("load data failed | error: %v", err)
			return
		}
	}

	log.Debugf("mongo DB initialization done")
	// Mongo DB follower initialization done

	listener, err := net.Listen("tcp", myAddr)
	if err != nil {
		log.Fatalf("ListenTCP error: %v", err)
		return
	}

	for {
		conn, err := listener.Accept()
		if err != nil {
			log.Fatal("Accept error:", err)
			return
		}

		go rpc.ServeConn(conn)
	}
}

package main

import (
	"cabinet/config"
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

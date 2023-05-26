package main

import (
	"cabinet/config"
	"net"
	"net/rpc"
)

var conns []ServerDock

type ServerDock struct {
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

	arith := NewCabService()
	err := rpc.Register(arith)
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

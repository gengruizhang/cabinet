package main

import (
	"cabinet/config"
	"net/rpc"
	"strconv"
	"sync"
)

func establishRPCs() {
	serverConfig := config.ParseClusterConfig(numOfServers, configPath)
	id := config.ServerID
	ip := config.ServerIP
	portOfRPCListener := config.ServerRPCListenerPort

	for i := 0; i < numOfServers; i++ {
		if i == myServerID {
			continue
		}

		serverID, err := strconv.Atoi(serverConfig[i][id])
		if err != nil {
			log.Errorf("%v", err)
			return
		}

		newServer := &ServerDock{
			serverID:   serverID,
			addr:       serverConfig[i][ip] + ":" + serverConfig[i][portOfRPCListener],
			txClient:   nil,
			prioClient: nil,
			jobQMu:     sync.RWMutex{},
			jobQ:       map[prioClock]chan struct{}{},
		}

		log.Infof("i: %d | newServer.addr: %v", i, newServer.addr)

		txClient, err := rpc.Dial("tcp", newServer.addr)
		if err != nil {
			log.Errorf("txClient rpc.Dial failed to %v | error: %v", newServer.addr, err)
			return
		}

		newServer.txClient = txClient

		prioClient, err := rpc.Dial("tcp", newServer.addr)

		if err != nil {
			log.Errorf("prioClient rpc.Dial failed to %v | error: %v", newServer.addr, err)
			return
		}

		newServer.prioClient = prioClient
		conns.Lock()
		conns.m[serverID] = newServer
		conns.Unlock()
	}
}

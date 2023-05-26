package main

import (
	"cabinet/config"
	"net/rpc"
)

func establishRPCs() {
	serverConfig := config.Parser(numOfServers, configPath)
	ip := config.ServerIP
	portOfRPCListener := config.ServerRPCListenerPort

	for i := 0; i < numOfServers; i++ {
		if i == myServerID {
			continue
		}

		newServer := ServerDock{
			addr:       serverConfig[i][ip] + ":" + serverConfig[i][portOfRPCListener],
			txClient:   nil,
			prioClient: nil,
		}

		log.Infof("i: %d | newServer.addr: %s", i, newServer.addr)

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
		conns = append(conns, newServer)
	}
}

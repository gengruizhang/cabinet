package main

import (
	"errors"
	"os/exec"
	"time"
)

const (
	PlainMsg = iota
	TPCC
	MongoDB
)

type CabService struct{}

func NewCabService() *CabService {
	return &CabService{}
}

type Args struct {
	PrioClock int
	PrioVal   float64
	Cmd       []string
	Type      int
}

type Reply struct {
	ServerID  int
	PrioClock int
	Result    int
	ExeTime   string
	ErrorMsg  error
}

func (s *CabService) ConsensusService(args *Args, reply *Reply) error {
	// 1. First update priority
	log.Infof("received args: %v", args)
	err := mypriority.UpdatePriority(args.PrioClock, args.PrioVal)
	if err != nil {
		log.Errorf("update priority failed | err: %v", err)
		reply.ErrorMsg = err
		return err
	}

	// 2. Then do transaction job
	switch args.Type {
	case PlainMsg:
		return conJobPlainMsg(args, reply)
	case TPCC:
		return conJobTPCC(args, reply)
	case MongoDB:
		return conJobMongoDB(args, reply)
	}

	err = errors.New("unidentified job")
	log.Errorf("err: %v | receievd type: %v", err, args.Type)
	return err
}

func conJobPlainMsg(args *Args, reply *Reply) (err error) {
	start := time.Now()

	err = exec.Command("python3", args.Cmd...).Run()

	if err != nil {
		log.Errorf("run cmd failed | err: %v", err)
		reply.ErrorMsg = err
		return
	}

	reply.ExeTime = time.Now().Sub(start).String()
	reply.ServerID = myServerID
	reply.PrioClock = mypriority.PrioClock
	return
}

func conJobTPCC(args *Args, reply *Reply) (err error) {
	err = errors.New("waiting for implementation")
	return
}

func conJobMongoDB(args *Args, reply *Reply) (err error) {
	err = errors.New("waiting for implementation")
	return
}

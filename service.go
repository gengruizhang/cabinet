package main

import (
	"cabinet/mongodb"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

type CabService struct{}

func NewCabService() *CabService {
	return &CabService{}
}

type Args struct {
	PrioClock int
	PrioVal   float64
	CmdPlain  []byte
	CmdMongo  []mongodb.Query
	// CmdTPCC  []string
	CmdPy []string
	Type  int
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
	// log.Infof("received args: %v", args)
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

	log.Infof("pClock: %v | msg: %x", args.PrioClock, args.CmdPlain)

	reply.ExeTime = time.Now().Sub(start).String()
	reply.ServerID = myServerID
	reply.PrioClock = mypriority.PrioClock

	return nil
}

func conJobPythonScript(args *Args, reply *Reply) (err error) {
	start := time.Now()

	err = exec.Command("python3", args.CmdPy...).Run()

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

	// do TPCC work
	err = errors.New("waiting for implementation")
	return
}

func conJobMongoDB(args *Args, reply *Reply) (err error) {

	// do mongoDB work
	// err = errors.New("waiting for implementation")

	//gob.Register([]mongodb.Query{})

	start := time.Now()

	queryResults, queryLatency, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
	if err != nil {
		log.Errorf("run cmd failed | err: %v", err)
		reply.ErrorMsg = err
		return
	}

	reply.ExeTime = time.Since(start).String()
	reply.ServerID = myServerID
	reply.PrioClock = mypriority.PrioClock

	// Print results...
	fmt.Println("Average latency of Mongo DB queries: ", queryLatency)
	for i, queryRes := range queryResults {
		if i >= 2 && i < len(queryResults)-3 {
			continue
		}
		fmt.Printf("\nResult of the %vth query: \n", i)
		for _, queRes := range queryRes {
			if uid, ok := queRes["_id"]; ok {
				fmt.Println("_id", "is", uid)
				delete(queRes, "_id")
			}
			for k, v := range queRes {
				fmt.Println(k, "is", v)
			}
		}
	}

	return
}

package main

import (
	"cabinet/mongodb"
	"cabinet/tpcc"
	"errors"
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
	CmdPlain  [][]byte
	CmdMongo  []mongodb.Query
	CmdTPCC   tpcc.TpccArgs
	CmdPy     []string
	Type      int
}

type ReplyInfo struct {
	SID    int
	PClock int
	Recv   Reply
}

type Reply struct {
	// ServerID and PrioClock are filled by leader
	// ServerID  int
	// PrioClock int

	ExeResult   string
	ErrorMsg    error
	TpccMetrics []map[string]string
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

	for _, msg := range args.CmdPlain {
		log.Infof("pClock: %v | msg: %x", args.PrioClock, msg)
	}

	reply.ExeResult = time.Now().Sub(start).String()
	//reply.ServerID = myServerID
	//reply.PrioClock = mypriority.PrioClock

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

	reply.ExeResult = time.Now().Sub(start).String()
	//reply.ServerID = myServerID
	//reply.PrioClock = mypriority.PrioClock
	return
}

func conJobTPCC(args *Args, reply *Reply) (err error) {

	log.Debugf("Server %d is executing PClock %d", myServerID, args.PrioClock)
	start := time.Now()
	metrics := make([]map[string]string, 0, 6)
	tpccFollower.TpccService(args.CmdTPCC, &metrics)
	//tpcc.TpccService(args.CmdTPCC)
	reply.ExeResult = time.Now().Sub(start).String()
	reply.TpccMetrics = metrics
	return
}

func conJobMongoDB(args *Args, reply *Reply) (err error) {

	//if myServerID == 1 || myServerID == 2 {
	//	time.Sleep(2 * time.Second)
	//}

	//gob.Register([]mongodb.Query{})
	log.Debugf("Server %d is executing PClock %d", myServerID, args.PrioClock)

	start := time.Now()

	_, queryLatency, err := mongoDbFollower.FollowerAPI(args.CmdMongo)
	if err != nil {
		log.Errorf("run cmd failed | err: %v | queryLatency %v", err, queryLatency)
		reply.ErrorMsg = err
		return
	}

	reply.ExeResult = time.Since(start).String()

	// //fmt.Println("Average latency of Mongo DB queries: ", queryLatency)
	// for i, queryRes := range queryResults {
	// 	if i >= 2 && i < len(queryResults)-3 {
	// 		continue
	// 	}
	// 	//fmt.Printf("\nResult of the %vth query: \n", i)
	// 	for _, queRes := range queryRes {
	// 		//if uid, ok := queRes["_id"]; ok {
	// 		if _, ok := queRes["_id"]; ok {
	// 			//fmt.Println("_id", "is", uid)
	// 			delete(queRes, "_id")
	// 		}
	// 		//for k, v := range queRes {
	// 		//	fmt.Println(k, "is", v)
	// 		//}
	// 	}
	// }

	log.Debugf("Server %d finished PClock %d", myServerID, args.PrioClock)

	return
}

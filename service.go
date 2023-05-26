package main

import (
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
	Prio      int
	Cmd       []string
}

type Reply struct {
	Result  int
	ExeTime string
	PriorityMgr
}

func (s *CabService) Add(args *Args, reply *Reply) error {
	reply.Result = args.PrioClock + args.Prio
	return nil
}

func (s *CabService) Subtract(args *Args, reply *Reply) error {
	reply.Result = args.PrioClock - args.Prio
	return nil
}

func (s *CabService) ExecuteScript(args *Args, reply *Reply) error {
	cmd := exec.Command("sh", args.Cmd...)

	start := time.Now()
	err := cmd.Run()

	reply.ExeTime = time.Now().Sub(start).String()

	if err != nil {
		return err
	}

	return nil
}

func (s *CabService) ExecutePython(args *Args, reply *Reply) error {

	cmd := exec.Command("python3", args.Cmd...)

	fmt.Println("cmd: ", cmd.String())

	start := time.Now()
	err := cmd.Run()

	reply.ExeTime = time.Now().Sub(start).String()
	reply.Prio = myPrio.Prio

	if err != nil {
		return err
	}

	return nil
}

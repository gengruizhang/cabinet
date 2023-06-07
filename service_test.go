package main

import (
	"testing"
)

func TestExecuteScript(t *testing.T) {
	cs := NewCabService()

	args := &Args{
		PrioClock: 0,
		PrioVal:   0,
		Cmd:       []string{"./sum.sh"},
	}

	reply := &Reply{}

	if err := cs.ExecuteScriptJob(args, reply); err != nil {
		t.Error(err)
	}
}

func TestExecutePython(t *testing.T) {
	cs := NewCabService()

	args := &Args{
		PrioClock: 0,
		PrioVal:   0,
		Cmd:       []string{"./sum.py", "100_000_000"},
	}

	reply := &Reply{}

	if err := cs.ExecutePythonJob(args, reply); err != nil {
		t.Error(err)
	}
}

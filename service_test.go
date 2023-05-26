package main

import (
	"testing"
)

func TestExecuteScript(t *testing.T) {
	cs := NewCabService()

	args := &Args{
		PrioClock: 0,
		Prio:      0,
		Cmd:       []string{"./sum.sh"},
	}

	reply := &Reply{}

	if err := cs.ExecuteScript(args, reply); err != nil {
		t.Error(err)
	}
}

func TestExecutePython(t *testing.T) {
	cs := NewCabService()

	args := &Args{
		PrioClock: 0,
		Prio:      0,
		Cmd:       []string{"./sum.py", "100_000_000"},
	}

	reply := &Reply{}

	if err := cs.ExecutePython(args, reply); err != nil {
		t.Error(err)
	}
}

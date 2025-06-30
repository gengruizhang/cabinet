package smr

import (
	"testing"
)

func TestInitThreeServers(t *testing.T) {
	n, f, b := 7, 2, 1
	var p PriorityManager
	p.Init(n, f, b, 0.001, true)

	scheme := p.scheme
	majority := p.majority
	cabPriority := sum(scheme[:f])
	oneLessCabPriority := sum(scheme[:f-1])

	if cabPriority <= majority {
		t.Errorf("cabPriority is less than majority | cabPriority: %v, majority: %v", cabPriority, majority)
		return
	}

	if oneLessCabPriority >= majority {
		t.Errorf("oneLessCabPriority is greater than majority | oneLessCabPriority: %v, majority: %v", oneLessCabPriority, majority)
		return
	}
	t.Logf("priority scheme: %+v", p.scheme)
	t.Logf("cabPriority: %v; they are: %+v", cabPriority, scheme[:f])
	t.Logf("majority: %v, oneLessCabPriority: %v", majority, oneLessCabPriority)
}

func TestInit(t *testing.T) {
	n, f, b := 10, 4, 1
	var p PriorityManager
	p.Init(n, f, b, 0.001, true)

	scheme := p.scheme
	majority := p.majority
	cabPriority := sum(scheme[:f])
	oneLessCabPriority := sum(scheme[:f-1])

	if cabPriority <= majority {
		t.Errorf("cabPriority is less than majority | cabPriority: %v, majority: %v", cabPriority, majority)
		return
	}

	if oneLessCabPriority >= majority {
		t.Errorf("oneLessCabPriority is greater than majority | oneLessCabPriority: %v, majority: %v", oneLessCabPriority, majority)
		return
	}
	t.Logf("priority scheme: %+v", p.scheme)
	t.Logf("cabPriority: %v; they are: %+v", cabPriority, scheme[:f])
	t.Logf("majority: %v, oneLessCabPriority: %v", majority, oneLessCabPriority)
}

func TestUpdateFollowerPriorities(t *testing.T) {
	n, f, b := 10, 4, 1
	leaderID := 0
	var p PriorityManager
	p.Init(n, f, b, 0.001, true)
	t.Logf("scheme: %+v\n", p.scheme)

	pC1 := 1

	pQ1 := make(chan serverID, n)
	pQ1 <- 5
	pQ1 <- 6
	pQ1 <- 7
	pQ1 <- 8

	err := p.UpdateFollowerPriorities(pC1, pQ1, leaderID)
	if err != nil {
		t.Error(err)
	}
	//t.Logf("pc %d | priorities: %+v", pC1, p.m[pC1])
	t.Logf("pc %d\n", pC1)
	for k, v := range p.m[pC1] {
		t.Logf("k: %d, v: %v\n", k, v)
	}

	pC2 := 2

	pQ2 := make(chan serverID, n)
	pQ2 <- 2
	pQ2 <- 4
	pQ2 <- 7

	err = p.UpdateFollowerPriorities(pC2, pQ2, leaderID)

	if err != nil {
		t.Error(err)
	}

	t.Logf("pc %d\n", pC2)
	for k, v := range p.m[pC2] {
		t.Logf("k: %d, v: %v\n", k, v)
	}
}

package main

import (
	"testing"
)

func TestInitPriorities(t *testing.T) {
	n, f, b := 10, 3, 1

	p, majority := initPriorities(n, f, b)
	cabPriority := sum(p[len(p)-f:])
	oneLessCabPriority := sum(p[len(p)-(f-1):])

	if cabPriority <= majority {
		t.Errorf("cabPriority is less than majority | cabPriority: %v, majority: %v", cabPriority, majority)
		return
	}

	if oneLessCabPriority >= majority {
		t.Errorf("oneLessCabPriority is greater than majority | oneLessCabPriority: %v, majority: %v", oneLessCabPriority, majority)
		return
	}
	t.Logf("priority scheme: %v", p)
	t.Logf("cabPriority: %v, majority: %v, oneLessCabPriority: %v", cabPriority, majority, oneLessCabPriority)
}

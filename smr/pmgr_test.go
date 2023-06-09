package smr

import (
	"testing"
)

func TestInit(t *testing.T) {
	n, f, b := 10, 3, 1
	var p PriorityManager
	p.Init(n, f, b)

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

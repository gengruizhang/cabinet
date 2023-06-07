package smr

import (
	"errors"
	"sync"
)

type PriorityState struct {
	sync.RWMutex
	PrioClock int
	PrioVal   float64
	Majority  float64
}

func NewServerPriority(initPrioClock int, initPrioVal float64) PriorityState {
	return PriorityState{
		PrioClock: initPrioClock,
		PrioVal:   initPrioVal,
	}
}

func (p *PriorityState) UpdatePriority(newPClock int, newPriority float64) error {
	p.Lock()
	defer p.Unlock()

	if newPClock < p.PrioClock {
		return errors.New("newPClock is less than current PClock")
	}
	p.PrioClock = newPClock
	p.PrioVal = newPriority

	return nil
}

func (p *PriorityState) GetPriority() (pClock int, pValue float64) {
	p.RLock()
	defer p.RUnlock()

	pClock = p.PrioClock
	pValue = p.PrioVal
	return
}

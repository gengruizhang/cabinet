package main

type PriorityMgr struct {
	PrioClock int
	Prio      int
}

func NewPrioMgr(pclock, prio int) PriorityMgr {
	return PriorityMgr{
		PrioClock: pclock,
		Prio:      prio,
	}
}

func (p *PriorityMgr) Update(pclock, prio int) (bool, int, int) {
	if pclock < p.PrioClock {
		return false, p.PrioClock, p.Prio
	}

	p.PrioClock = pclock
	p.Prio = prio

	return true, p.PrioClock, p.Prio
}

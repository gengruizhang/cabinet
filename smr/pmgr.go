package smr

import (
	"errors"
	"fmt"
	"math"
	"sync"
)

type serverID = int
type prioClock = int
type priority = float64

type PriorityManager struct {
	sync.RWMutex
	m        map[prioClock]map[serverID]priority
	scheme   []priority
	majority float64
	n        int
	f        int
}

func (pm *PriorityManager) Init(numOfServers, numOfFaults, baseOfPriorities int) {
	pm.n = numOfServers
	pm.f = numOfFaults
	pm.m = make(map[prioClock]map[serverID]priority)

	ratio := calcInitPrioRatio(numOfServers, numOfFaults)
	fmt.Println("ratio: ", ratio)

	newPriorities := make(map[serverID]priority)

	for i := 0; i < numOfServers; i++ {
		p := float64(baseOfPriorities) * math.Pow(ratio, float64(i))
		newPriorities[i] = p
		pm.scheme = append(pm.scheme, p)
	}

	reverseSlice(pm.scheme)

	pm.majority = sum(pm.scheme) / 2

	pm.Lock()
	pm.m[0] = newPriorities
	pm.Unlock()
	return
}

func reverseSlice(slice []priority) {
	length := len(slice)
	for i := 0; i < length/2; i++ {
		j := length - 1 - i
		slice[i], slice[j] = slice[j], slice[i]
	}
}

func calcInitPrioRatio(n, f int) (ratio float64) {
	r := 2.0 // initial guess
	for {
		if math.Pow(r, float64(n-f+1)) > 0.5*(math.Pow(r, float64(n))+1) && 0.5*(math.Pow(r, float64(n))+1) > math.Pow(r, float64(n-f)) {
			return r
		} else {
			r -= 0.001
		}
	}
}

func sum(arr []float64) float64 {
	total := 0.0
	for _, val := range arr {
		total += val
	}
	return total
}

func (pm *PriorityManager) UpdateFollowerPriorities(pClock prioClock, prioQueue chan serverID, leaderID serverID) error {

	newPriorities := make(map[serverID]priority)
	arranged := make(map[serverID]bool)

	for i := 0; i < pm.n; i++ {
		arranged[i] = false
	}

	nr := len(prioQueue)

	for i := 0; i < nr; i++ {
		s := <-prioQueue
		// skip leader
		newPriorities[s] = pm.scheme[i+1]

		arranged[s] = true

		//fmt.Printf("pc: %d | processing %d is done | i is: %d | arranged %+v \n ", pClock, s, i, arranged)
	}

	i := nr + 1

	for id, done := range arranged {
		if !done {
			if id == leaderID {
				newPriorities[id] = pm.scheme[0]
				continue
			}

			if i == len(pm.scheme) {
				err := fmt.Sprintf("priority assignment of [%v] exceeds pm scheme length [%v]", i, len(pm.scheme))
				return errors.New(err)
			}
			newPriorities[id] = pm.scheme[i]
			i++
		}
	}

	pm.Lock()
	pm.m[pClock] = newPriorities
	pm.Unlock()
	//fmt.Printf("newPriorities: %+v\n", newPriorities)
	return nil
}

func (pm *PriorityManager) GetFollowerPriorities(pClock int) (fpriorities map[serverID]priority) {
	fpriorities = make(map[serverID]priority)

	pm.RLock()
	defer pm.RUnlock()

	fpriorities = pm.m[pClock]
	return
}

func (pm *PriorityManager) GetMajority() (majority float64) {
	majority = pm.majority
	return
}

func (pm *PriorityManager) GetPriorityScheme() (scheme []priority) {
	scheme = pm.scheme
	return
}

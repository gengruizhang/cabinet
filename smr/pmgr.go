package smr

import (
	"fmt"
	"math"
	"sync"
)

// 1, 2, 3, 4, 5
// pManager is used to manage follower priorities by the leader
//
//	var pManager = struct {
//		sync.RWMutex
//		m map[prioClock]map[serverID]priority
//	}{m: make(map[prioClock]map[serverID]priority)}

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
			r -= 0.01
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

func (pm *PriorityManager) UpdateFollowerPriorities(pClock int, prioQueue chan serverID) {

	newPriorities := make(map[serverID]priority)
	serverIDIndicator := make([]serverID, pm.n)

	for i := 0; i < len(serverIDIndicator); i++ {
		serverIDIndicator[i] = i
	}

	for i := 0; i < pm.f-1; i++ {
		s := <-prioQueue
		newPriorities[s] = pm.scheme[i+1]

		index := -1
		for i, id := range serverIDIndicator {
			if id == s {
				index = i
				break
			}
		}

		// Remove the ID from the slice if found
		if index != -1 {
			serverIDIndicator = append(serverIDIndicator[1:index], serverIDIndicator[index+1:]...)
		}

	}

	i := pm.f
	for _, id := range serverIDIndicator {
		newPriorities[id] = pm.scheme[i]
		i++
	}

	pm.Lock()
	pm.m[pClock] = newPriorities
	pm.Unlock()
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

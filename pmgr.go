package main

import (
	"fmt"
	"math"
	"sync"
)

// 1, 2, 3, 4, 5
// pManager is used to manage follower priorities by the leader
var pManager = struct {
	sync.RWMutex
	m map[prioClock]map[serverID]priority
}{m: make(map[prioClock]map[serverID]priority)}

func initPriorities(n, f, b int) (prioScheme []priority, majority float64) {

	ratio := calcInitPrioRatio(n, f)
	fmt.Println("ratio: ", ratio)

	newPriorities := make(map[serverID]priority)

	for i := 0; i < n; i++ {
		p := float64(b) * math.Pow(ratio, float64(i))
		newPriorities[i] = p
		prioScheme = append(prioScheme, p)
	}

	reverseSlice(prioScheme)

	majority = sum(prioScheme) / 2

	pManager.Lock()
	pManager.m[0] = newPriorities
	pManager.Unlock()
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

func updateFollowerPriorities(pClock int, prioQueue chan serverID) {

	newPriorities := make(map[serverID]priority)
	serverIDIndicator := make([]serverID, numOfServers)

	for i := 0; i < len(serverIDIndicator); i++ {
		serverIDIndicator[i] = i
	}

	if len(pscheme) != numOfServers {
		log.Errorf("pscheme len is not numOfServers | len: %v, n: %v", len(pscheme), numOfServers)
		return
	}

	for i := 0; i < faults-1; i++ {
		s := <-prioQueue
		log.Debugf("fetched prioQueue for: %v", s)
		newPriorities[s] = pscheme[i+1]

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

	log.Infof("remaining servers from prioritization: %v", serverIDIndicator)

	i := faults
	for _, id := range serverIDIndicator {
		newPriorities[id] = pscheme[i]
		i++
	}

	log.Infof("newPriorities for PClock %v: %v", pClock, newPriorities)
	pManager.Lock()
	pManager.m[pClock] = newPriorities
	pManager.Unlock()
}

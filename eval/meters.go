package eval

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

type serverID = int
type prioClock = int
type priority = float64

type PerfMeter struct {
	sync.RWMutex
	numOfTotalTx   int
	batchSize      int
	sampleInterval prioClock
	lastPClock     prioClock
	fileName       string
	meters         map[prioClock]*RecordInstance
}

type RecordInstance struct {
	StartTime time.Time
	//EndTime     time.Time
	TimeElapsed int64
}

func (m *PerfMeter) Init(interval, batchSize int, fileName string) {
	m.sampleInterval = interval
	m.lastPClock = 0
	m.batchSize = batchSize
	m.numOfTotalTx = 0
	m.fileName = fileName
	m.meters = make(map[prioClock]*RecordInstance)
}

func (m *PerfMeter) RecordStarter(pClock prioClock) {

	m.Lock()
	defer m.Unlock()

	m.meters[pClock] = &RecordInstance{
		StartTime: time.Now(),
		//EndTime:     time.Time{},
		TimeElapsed: 0,
	}
}

func (m *PerfMeter) RecordFinisher(pClock prioClock) error {
	m.Lock()
	defer m.Unlock()

	_, exist := m.meters[pClock]
	if !exist {
		return errors.New("pClock has not been recorded with starter")
	}

	//m.meters[pClock].EndTime = time.Now()
	start := m.meters[pClock].StartTime
	m.meters[pClock].TimeElapsed = time.Now().Sub(start).Milliseconds()

	return nil
}

func (m *PerfMeter) SaveToFile() error {

	file, err := os.Create(fmt.Sprintf("./eval/%s.csv", m.fileName))
	if err != nil {
		return err
	}

	defer file.Close()

	// Create a CSV writer
	writer := csv.NewWriter(file)
	defer writer.Flush()

	m.RLock()
	defer m.RUnlock()

	var keys []int
	for key := range m.meters {
		keys = append(keys, key)
	}
	sort.Ints(keys)

	err = writer.Write([]string{"pclock", "latency (ms) per batch", "throughput (Tx per Second)"})
	if err != nil {
		return err
	}

	counter := 0
	var latSum int64 = 0

	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}

		latSum += value.TimeElapsed
		counter++

		lat := value.TimeElapsed
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
		row := []string{strconv.Itoa(key), strconv.FormatInt(lat, 10), strconv.FormatFloat(tpt, 'f', 3, 64)}

		err := writer.Write(row)
		if err != nil {
			return err
		}
	}

	if counter == 0 {
		return errors.New("counter is 0")
	}

	avgLatency := float64(latSum) / float64(counter)
	lastTime := m.meters[keys[len(keys)-1]]
	lastEndTime := lastTime.StartTime.Add(time.Duration(time.Duration(lastTime.TimeElapsed).Seconds()))
	avgThroughput := float64(m.batchSize*counter) / lastEndTime.Sub(m.meters[keys[0]].StartTime).Seconds()

	err = writer.Write([]string{"-1", strconv.FormatFloat(avgLatency, 'f', 3, 64), strconv.FormatFloat(avgThroughput, 'f', 3, 64)})
	if err != nil {
		return err
	}

	return nil
}

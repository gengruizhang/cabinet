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

type TxnMetric struct {
	Name       string
	Count      int
	AvgExecLat float64
	AvgTPM     float64
}

type RecordInstance struct {
	StartTime time.Time
	//EndTime     time.Time
	TimeElapsed int64
	Tpccmetrics map[string]*TxnMetric
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
		Tpccmetrics: make(map[string]*TxnMetric),
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

func (m *PerfMeter) RecordTpccTxnMetrics(pClock prioClock, txnMetric map[string]string, avgTPM float64, avgExecLat float64) {

	//Check if we are looking for a transaction type or the total metrics
	if _, exists := txnMetric["Operation"]; exists {
		count, _ := strconv.Atoi(txnMetric["Count"])
		//lat, _ := strconv.ParseFloat(txnMetric["Avg(ms)"], 64)
		//tpm, _ := strconv.ParseFloat(txnMetric["TPM"], 64)
		m.meters[pClock].Tpccmetrics[txnMetric["Operation"]] = &TxnMetric{
			Name:       txnMetric["Operation"],
			Count:      count,
			AvgExecLat: avgExecLat,
			AvgTPM:     avgTPM,
		}
	}

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

	avgLatency := float64(latSum / int64(counter))
	avgThroughput := float64(m.batchSize*counter) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()

	err = writer.Write([]string{"-1", strconv.FormatFloat(avgLatency, 'f', 3, 64), strconv.FormatFloat(avgThroughput, 'f', 3, 64)})
	if err != nil {
		return err
	}

	return nil
}

func (m *PerfMeter) SaveToFileTpcc() error {

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

	err = writer.Write([]string{"pclock", "consensus latency (ms) per batch", "throughput (Tx per Second)",
		"DELIVERY throughput(leader side-Tx per Second)", "DELIVERY throughput(follower side-Tx per Second)", "DELIVERY execution latency(ms)",
		"NEW_ORDER throughput(leader side-Tx per Second)", "NEW_ORDER throughput(follower side-Tx per Second)", "NEW_ORDER execution latency(ms)",
		"ORDER_STATUS throughput(leader side-Tx per Second)", "ORDER_STATUS throughput(follower side-Tx per Second)", "ORDER_STATUS execution latency(ms)",
		"PAYMENT throughput(leader side-Tx per Second)", "PAYMENT throughput(follower side-Tx per Second)", "PAYMENT execution latency(ms)",
		"STOCK_LEVEL throughput(leader side-Tx per Second)", "STOCK_LEVEL throughput(follower side-Tx per Second)", "STOCK_LEVEL execution latency(ms)"})
	if err != nil {
		return err
	}

	counter := 0
	var latSum int64 = 0

	//TPCC counters
	var deliverySum int = 0
	var newOrderSum int = 0
	var orderStatusSum int = 0
	var paymentSum int = 0
	var stockLevelSum int = 0

	//TPCC average execution latency counters for each transaction
	var deliveryExecLatSum float64 = 0.0
	var newOrderExecLatSum float64 = 0.0
	var orderStatusExecLatSum float64 = 0.0
	var paymentExecLatSum float64 = 0.0
	var stockLevelExecLatSum float64 = 0.0

	//TPCC average execution throughput counters for each transaction
	var deliveryExecTptSum float64 = 0.0
	var newOrderExecTptSum float64 = 0.0
	var orderStatusExecTptSum float64 = 0.0
	var paymentExecTptSum float64 = 0.0
	var stockLevelExecTptSum float64 = 0.0

	for _, key := range keys {
		value := m.meters[key]
		if value.TimeElapsed == 0 {
			continue
		}

		latSum += value.TimeElapsed
		counter++

		lat := value.TimeElapsed
		tpt := (float64(m.batchSize) / float64(lat)) * 1000
		var row []string
		deliverySum += value.Tpccmetrics["DELIVERY"].Count
		newOrderSum += value.Tpccmetrics["NEW_ORDER"].Count
		orderStatusSum += value.Tpccmetrics["ORDER_STATUS"].Count
		paymentSum += value.Tpccmetrics["PAYMENT"].Count
		stockLevelSum += value.Tpccmetrics["STOCK_LEVEL"].Count
		tptDelivery := (float64(value.Tpccmetrics["DELIVERY"].Count) / float64(lat)) * 1000
		tptNeworder := (float64(value.Tpccmetrics["NEW_ORDER"].Count) / float64(lat)) * 1000
		tptOrderstatus := (float64(value.Tpccmetrics["ORDER_STATUS"].Count) / float64(lat)) * 1000
		tptPayment := (float64(value.Tpccmetrics["PAYMENT"].Count) / float64(lat)) * 1000
		tptStocklevel := (float64(value.Tpccmetrics["STOCK_LEVEL"].Count) / float64(lat)) * 1000

		deliveryExecLatSum += value.Tpccmetrics["DELIVERY"].AvgExecLat
		newOrderExecLatSum += value.Tpccmetrics["NEW_ORDER"].AvgExecLat
		orderStatusExecLatSum += value.Tpccmetrics["ORDER_STATUS"].AvgExecLat
		paymentExecLatSum += value.Tpccmetrics["PAYMENT"].AvgExecLat
		stockLevelExecLatSum += value.Tpccmetrics["STOCK_LEVEL"].AvgExecLat

		deliveryExecTptSum += value.Tpccmetrics["DELIVERY"].AvgTPM
		newOrderExecTptSum += value.Tpccmetrics["NEW_ORDER"].AvgTPM
		orderStatusExecTptSum += value.Tpccmetrics["ORDER_STATUS"].AvgTPM
		paymentExecTptSum += value.Tpccmetrics["PAYMENT"].AvgTPM
		stockLevelExecTptSum += value.Tpccmetrics["STOCK_LEVEL"].AvgTPM

		row = []string{strconv.Itoa(key), strconv.FormatInt(lat, 10), strconv.FormatFloat(tpt, 'f', 3, 64),
			strconv.FormatFloat(tptDelivery, 'f', 3, 64), strconv.FormatFloat(value.Tpccmetrics["DELIVERY"].AvgTPM/60.0, 'f', 3, 64),
			strconv.FormatFloat(value.Tpccmetrics["DELIVERY"].AvgExecLat, 'f', 3, 64),
			strconv.FormatFloat(tptNeworder, 'f', 3, 64), strconv.FormatFloat(value.Tpccmetrics["NEW_ORDER"].AvgTPM/60.0, 'f', 3, 64),
			strconv.FormatFloat(value.Tpccmetrics["NEW_ORDER"].AvgExecLat, 'f', 3, 64),
			strconv.FormatFloat(tptOrderstatus, 'f', 3, 64), strconv.FormatFloat(value.Tpccmetrics["ORDER_STATUS"].AvgTPM/60.0, 'f', 3, 64),
			strconv.FormatFloat(value.Tpccmetrics["ORDER_STATUS"].AvgExecLat, 'f', 3, 64),
			strconv.FormatFloat(tptPayment, 'f', 3, 64), strconv.FormatFloat(value.Tpccmetrics["PAYMENT"].AvgTPM/60.0, 'f', 3, 64),
			strconv.FormatFloat(value.Tpccmetrics["PAYMENT"].AvgExecLat, 'f', 3, 64),
			strconv.FormatFloat(tptStocklevel, 'f', 3, 64), strconv.FormatFloat(value.Tpccmetrics["STOCK_LEVEL"].AvgTPM/60.0, 'f', 3, 64),
			strconv.FormatFloat(value.Tpccmetrics["STOCK_LEVEL"].AvgExecLat, 'f', 3, 64)}

		err := writer.Write(row)
		if err != nil {
			return err
		}
	}

	if counter == 0 {
		return errors.New("counter is 0")
	}

	avgLatency := float64(latSum / int64(counter))
	avgThroughput := float64(m.batchSize*counter) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()

	avgThroughputDelivery := float64(deliverySum) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()
	avgThroughputNewOrder := float64(newOrderSum) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()
	avgThroughputOrderStatus := float64(orderStatusSum) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()
	avgThroughputPayment := float64(paymentSum) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()
	avgThroughputStockLevel := float64(stockLevelSum) / time.Now().Sub(m.meters[keys[0]].StartTime).Seconds()

	avgDeliveryExecLat := deliveryExecLatSum / float64(len(keys)-1)
	avgNewOrderExecLat := newOrderExecLatSum / float64(len(keys)-1)
	avgOrderStatusExecLat := orderStatusExecLatSum / float64(len(keys)-1)
	avgPaymentExecLat := paymentExecLatSum / float64(len(keys)-1)
	avgStockLevelExecLat := stockLevelExecLatSum / float64(len(keys)-1)

	avgDeliveryExecTpt := (deliveryExecTptSum / float64(len(keys)-1)) / 60
	avgNewOrderExecTpt := (newOrderExecTptSum / float64(len(keys)-1)) / 60
	avgOrderStatusExecTpt := (orderStatusExecTptSum / float64(len(keys)-1)) / 60
	avgPaymentExecTpt := (paymentExecTptSum / float64(len(keys)-1)) / 60
	avgStockLevelExecTpt := (stockLevelExecTptSum / float64(len(keys)-1)) / 60

	err = writer.Write([]string{"-1", strconv.FormatFloat(avgLatency, 'f', 3, 64), strconv.FormatFloat(avgThroughput, 'f', 3, 64),
		strconv.FormatFloat(avgThroughputDelivery, 'f', 3, 64), strconv.FormatFloat(avgDeliveryExecTpt, 'f', 3, 64),
		strconv.FormatFloat(avgDeliveryExecLat, 'f', 3, 64),
		strconv.FormatFloat(avgThroughputNewOrder, 'f', 3, 64), strconv.FormatFloat(avgNewOrderExecTpt, 'f', 3, 64),
		strconv.FormatFloat(avgNewOrderExecLat, 'f', 3, 64),
		strconv.FormatFloat(avgThroughputOrderStatus, 'f', 3, 64), strconv.FormatFloat(avgOrderStatusExecTpt, 'f', 3, 64),
		strconv.FormatFloat(avgOrderStatusExecLat, 'f', 3, 64),
		strconv.FormatFloat(avgThroughputPayment, 'f', 3, 64), strconv.FormatFloat(avgPaymentExecTpt, 'f', 3, 64),
		strconv.FormatFloat(avgPaymentExecLat, 'f', 3, 64),
		strconv.FormatFloat(avgThroughputStockLevel, 'f', 3, 64), strconv.FormatFloat(avgStockLevelExecTpt, 'f', 3, 64),
		strconv.FormatFloat(avgStockLevelExecLat, 'f', 3, 64)})
	if err != nil {
		return err
	}

	return nil
}

package mongodb

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var log = logrus.New()

const DataPath string = "./ycsb/workData/"

type MongoFollower struct {
	clientThreadNum int
	queryThreadNum  int

	dbName string

	clients []*mongo.Client
}

func NewMongoFollower(clientTNum int, queryTNum int, dbID int) *MongoFollower {
	if clientTNum <= 0 || queryTNum <= 0 {
		err := errors.New("thread number should be positive integer")
		log.Errorf("create MongoDB Follower failed | clientThreadNum: %v, queryThreadNum: %v, err: %v",
			clientTNum, queryTNum, err)
		return nil
	}

	follower := &MongoFollower{
		clientThreadNum: clientTNum,
		queryThreadNum:  queryTNum,
	}

	if dbID == 0 {
		follower.dbName = "ycsb"
	} else {
		follower.dbName = "ycsb" + strconv.Itoa(dbID)
	}

	// build MongoDB clients
	uri := os.Getenv("MONGODB_URI")
	log.Debugf("mongodb url: %v", uri)
	if uri == "" {
		uri = "mongodb://localhost:27017/"
		// log.Fatal("You must set your 'MONGODB_URI' environmental variable.
		//			See\n\t https://www.mongodb.com/docs/drivers/go/current/usage-examples/#environment-variable")
	}

	for i := 0; i < clientTNum; i++ {
		cli, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
		if err != nil {
			log.Errorf("create MongoDB client failed | err: %v", err)
			return nil
		}
		follower.clients = append(follower.clients, cli)
	}

	return follower
}

func (fl *MongoFollower) FollowerAPI(queries []Query) (
	result [][]map[string]string, latency time.Duration, err error) {

	var wg sync.WaitGroup
	mu := &sync.Mutex{}

	queriesPerClient := make([][]Query, fl.clientThreadNum)
	if fl.clientThreadNum == 1 {
		queriesPerClient[0] = queries
	} else {
		for i := 0; i < len(queries); i++ {
			clientToAppend := i % fl.clientThreadNum
			queriesPerClient[clientToAppend] = append(queriesPerClient[clientToAppend], queries[i])
		}
	}

	wg.Add(fl.clientThreadNum)
	for i := 0; i < fl.clientThreadNum; i++ {
		go func(i int) {
			defer wg.Done()

			db := fl.clients[i].Database(fl.dbName)
			resultsPerClient, latPerClient, err := followerClient(db, queriesPerClient[i], fl.queryThreadNum)
			if err != nil {
				log.Fatal(err)
			}

			mu.Lock()
			latency += latPerClient
			result = append(result, resultsPerClient...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	latency = time.Duration(float64(latency) / float64(fl.clientThreadNum))
	return result, latency, nil
}

func (fl *MongoFollower) ClearTable(table string) (err error) {

	dropTable := Query{
		Op:     DROP,
		Table:  table,
		Key:    "",
		Values: nil,
	}

	_, _, err = fl.FollowerAPI([]Query{dropTable})

	if err != nil {
		log.Errorf("clear table failed | err: %v")
		return
	}
	log.Debugf("table cleared: %s", table)

	return
}

func (fl *MongoFollower) PrintTable(table string) (err error) {

	findAll := Query{
		Op:     READ,
		Table:  table,
		Key:    "",
		Values: map[string]string{"<all fields>": ""},
	}
	findAllRes, _, err := fl.FollowerAPI([]Query{findAll})
	if err != nil {
		return
	}

	log.Println("Current entries in table ", table)
	for i, result := range findAllRes[0] {
		if i >= 10 || i >= len(findAllRes[0])-1 {
			break
		}
		log.Printf("========== entry %v ==========\n", i)

		idVal, ok := result["_id"]
		if !ok {
			err = errors.New("User ID does not exist")
			log.Errorf("print table failed | err: %v", err)
			return
		}
		log.Println("_id", "has value", idVal)
		delete(result, "_id")

		for k, v := range result {
			log.Println(k, "has value", v)
		}
	}
	return
}

func (fl *MongoFollower) CleanUp() (err error) {
	err = fl.ClearTable("usertable")
	if err != nil {
		log.Errorf("clean up table failed | err: %v", err)
		return
	}

	for _, cli := range fl.clients {
		err = cli.Disconnect(context.TODO())
		if err != nil {
			log.Errorf("disconnect Mongo DB clients failed | err: %v", err)
			return
		}
	}

	return
}

func followerClient(db *mongo.Database, queries []Query, qThreadNum int) (
	results [][]map[string]string, latency time.Duration, err error) {

	if qThreadNum == 1 {
		// Single thread
		for _, query := range queries {
			startTime := time.Now()
			result, err := queryHandler(db, query)
			if err != nil {
				return nil, time.Duration(0), err
			}
			latency += time.Since(startTime)
			results = append(results, result)
		}
	} else {
		// Multi-thread
		var wg sync.WaitGroup
		mu := &sync.Mutex{}

		// concurrent load
		qBuf := make(chan Query, len(queries))
		wg.Add(1)
		go func() {
			defer close(qBuf)
			defer wg.Done()
			for _, q := range queries {
				qBuf <- q
			}
		}()

		wg.Add(qThreadNum)
		for i := 0; i < qThreadNum; i++ {
			go func() {
				defer wg.Done()
				for query := range qBuf {
					startTime := time.Now()
					result, err := queryHandler(db, query)
					if err != nil {
						log.Fatal(err)
					}

					mu.Lock()
					latency += time.Since(startTime)
					results = append(results, result)
					mu.Unlock()
				}
			}()
		}

		wg.Wait()
	}
	latency = time.Duration(float64(latency) / float64(len(queries)))
	return results, latency, nil
}

func queryHandler(db *mongo.Database, query Query) (opRes []map[string]string, err error) {

	switch query.Op {
	case INSERT:
		if err = dbInsert(db, query.Table, query.Key, query.Values); err != nil {
			return nil, err
		}
		opRes = append(opRes, map[string]string{"INSERT": "done"})
	case READ:
		opRes, err = dbRead(db, query.Table, query.Key, query.Values)
		if err != nil {
			return nil, err
		}
		opRes = append(opRes, map[string]string{"READ": "done"})
	case UPDATE:
		if err = dbUpdate(db, query.Table, query.Key, query.Values); err != nil {
			return nil, err
		}
		opRes = append(opRes, map[string]string{"UPDATE": "done"})
	case SCAN:
		scanCount, ok := query.Values["<all fields>"]
		if !ok || scanCount == "" {
			return nil, fmt.Errorf("Unexpected scan count")
		}
		recordCount, err := strconv.Atoi(scanCount)
		if err != nil {
			return nil, err
		}

		opRes, err = dbScan(db, query.Table, query.Key, recordCount, query.Values)
		if err != nil {
			return nil, err
		}
		opRes = append(opRes, map[string]string{"SCAN": "done"})
	case DELETE:
		dbDelete(db, query.Table, query.Key)
		opRes = append(opRes, map[string]string{"DELETE": "done"})
	case DROP:
		dbDrop(db, query.Table)
		opRes = append(opRes, map[string]string{"DROP": "done"})
	default:
		return nil, fmt.Errorf("Unexpected operator: %s\n", strconv.Itoa(query.Op))
	}
	return opRes, nil
}

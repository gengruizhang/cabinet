package mongodb

import (
	"context"
	"fmt"
	"os"
	"reflect"
	"runtime"
	"strconv"
	"strings"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func main() {

	filePath := "../testData/workData/"
	fileName := ""
	clientThreadNum, queryThreadNum := 1, 1
	runTime := true
	var err error
	var startTime time.Time
	mgFollower := NewMongoFollower(clientThreadNum, queryThreadNum, 0)

	// defer func() { fmt.Println(timeTests(buildClientTest)) }()

	// Parsing parameters
	if len(os.Args) > 1 {
		param := os.Args[1]
		if strings.HasSuffix(param, ".dat") {
			fileName = param
		} else {
			switch param {
			case "load":
				fileName = "load_workloada.dat"
			case "run":
				fileName = "run_workloada.dat"
			case "clear":
				if err := mgFollower.ClearTable("usertable"); err != nil {
					log.Fatal(err)
				}
				return
			case "print":
				if err := mgFollower.PrintTable("usertable"); err != nil {
					log.Fatal(err)
				}
				return
			default:
				fmt.Println("Unrecognized parameter")
				fileName = "workloada.dat"
			}
		}
	}
	if strings.Contains(fileName, "run") {
		runTime = true
	}
	fn := filePath + fileName

	if len(os.Args) > 3 {
		clientThreadNum, err = strconv.Atoi(os.Args[2])
		if err != nil {
			log.Fatal(err)
		}
		queryThreadNum, err = strconv.Atoi(os.Args[3])
		if err != nil {
			log.Fatal(err)
		}
	}

	// Get queries [leader.go]
	queries, err := ReadQueryFromFile(fn)
	if err != nil {
		log.Fatal(err)
	}

	// Print queries...
	// printq := append(queries[:5], queries[len(queries)-5:]...)
	// for _, q := range printq {
	// 	printQuery(q)
	// }

	if runTime {
		startTime = time.Now()
	}

	// Send queries to follower [follower.go]
	queryResults, qLatency, err := mgFollower.FollowerAPI(queries)
	if err != nil {
		log.Fatal(err)
	}

	if runTime {
		totalTime := time.Since(startTime)
		throughput := float64(len(queries)) * float64(time.Second) / float64(totalTime)
		defer func() {
			fmt.Printf("\n==========  RESULTS  ==========\n\n")
			fmt.Printf("Data file: %s\n", fileName)
			fmt.Printf("# clients: %v\n", clientThreadNum)
			fmt.Printf("# threads per client: %v\n", queryThreadNum)
			fmt.Printf("Latency in MongoDB(follower): %v\n", qLatency)
			fmt.Printf("Total time(s): %v; Throughput(tps): %v\n\n", totalTime, throughput)
		}()
	}

	// Print results...
	for i, queryRes := range queryResults {
		if i >= 2 && i < len(queryResults)-3 {
			continue
		}
		fmt.Printf("\nResult of the %vth query: \n", i)
		for _, queRes := range queryRes {
			if uid, ok := queRes["_id"]; ok {
				fmt.Println("_id", "is", uid)
				delete(queRes, "_id")
			}
			for k, v := range queRes {
				fmt.Println(k, "is", v)
			}
		}
	}
}

func timeTests(testFunc func()) time.Duration {
	startTime := time.Now()
	testFunc()
	runTime := time.Since(startTime)
	fmt.Printf("Function %s runtime: %s\n",
		runtime.FuncForPC(reflect.ValueOf(testFunc).Pointer()).Name(), runTime)

	return runTime
}

func buildClientTest() {
	clientNum := 64
	uri := "mongodb://localhost:27017/"
	queries := make([]Query, 10000*clientNum)

	buildClients(uri, clientNum, queries)
}

func buildClients(uri string, clientNum int, queries []Query) []*mongo.Client {
	clients := make([]*mongo.Client, clientNum)
	for i := 0; i < clientNum; i++ {
		client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))
		if err != nil {
			log.Fatal(err)
		}
		defer func() {
			if err := client.Disconnect(context.TODO()); err != nil {
				log.Fatal(err)
			}
		}()

		clients = append(clients, client)
	}
	queriesPerClient := make([][]Query, clientNum)
	for i := 0; i < len(queries); i++ {
		clientToAppend := i % clientNum
		queriesPerClient[clientToAppend] =
			append(queriesPerClient[clientToAppend], queries[i])
	}

	return clients
}

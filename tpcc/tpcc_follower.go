package tpcc

import (
	"crypto/sha1"
	"database/sql"
	sqldrv "database/sql/driver"
	"encoding/hex"
	"errors"
	"fmt"
	"github.com/go-sql-driver/mysql"
	"github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"strconv"
	"strings"
	"sync/atomic"
)

var log = logrus.New()

var (
	clientDB *sql.DB
)

type TPCCFollower struct {
	clientThreadNum int
	dbName          string

	dbconnections []*sql.DB
}

const (
	createDBDDL = "CREATE DATABASE "
	mysqlDriver = "mysql"
	pgDriver    = "postgres"
)

var Results []map[string]string

type MuxDriver struct {
	cursor    uint64
	instances []string
	internal  sqldrv.Driver
}

func (drv *MuxDriver) Open(name string) (sqldrv.Conn, error) {
	k := atomic.AddUint64(&drv.cursor, 1)
	return drv.internal.Open(drv.instances[int(k)%len(drv.instances)])
}

func makeTargets(hosts []string, ports []int) []string {
	targets := make([]string, 0, len(hosts)*len(ports))
	for _, host := range hosts {
		for _, port := range ports {
			targets = append(targets, host+":"+strconv.Itoa(port))
		}
	}
	return targets
}

func NewTpccFollower(tpccConfig Config) *TPCCFollower {

	follower := &TPCCFollower{
		clientThreadNum: tpccConfig.Threads,
	}

	for i := 0; i < tpccConfig.Threads; i++ {
		clientDB, err := OpenDB(tpccConfig)
		if err != nil {
			log.Errorf("create MongoDB client failed | err: %v", err)
			return nil
		}
		follower.dbconnections = append(follower.dbconnections, clientDB)
	}

	return follower

}

func newDB(targets []string, driver string, user string, password string, dbName string, connParams string) (*sql.DB, error) {
	if len(targets) == 0 {
		panic(fmt.Errorf("empty targets"))
	}
	var (
		drv   sqldrv.Driver
		hash  = sha1.New()
		names = make([]string, len(targets))
	)
	hash.Write([]byte(driver))
	hash.Write([]byte(user))
	hash.Write([]byte(password))
	hash.Write([]byte(dbName))
	hash.Write([]byte(connParams))
	for i, addr := range targets {
		hash.Write([]byte(addr))
		switch driver {
		case mysqlDriver:
			// allow multiple statements in one query to allow q15 on the TPC-H
			dsn := fmt.Sprintf("%s:%s@tcp(%s)/%s?multiStatements=true", user, password, addr, dbName)
			if len(connParams) > 0 {
				dsn = dsn + "&" + connParams
			}
			names[i] = dsn
			drv = &mysql.MySQLDriver{}
		case pgDriver:
			dsn := fmt.Sprintf("postgres://%s:%s@%s/%s", user, password, addr, dbName)
			if len(connParams) > 0 {
				dsn = dsn + "?" + connParams
			}
			names[i] = dsn
			drv = &pq.Driver{}
		default:
			panic(fmt.Errorf("unknown driver: %q", driver))
		}
	}

	if len(names) == 1 {
		return sql.Open(driver, names[0])
	}
	drvName := driver + "+" + hex.EncodeToString(hash.Sum(nil))
	for _, n := range sql.Drivers() {
		if n == drvName {
			return sql.Open(drvName, "")
		}
	}
	sql.Register(drvName, &MuxDriver{instances: names, internal: drv})
	return sql.Open(drvName, "")
}

func closeDB(clientDB *sql.DB) (err error) {
	if clientDB != nil {
		err = clientDB.Close()
		if err != nil {
			log.Errorf("close client db failed | err: %v", err)
			return
		}
	}
	clientDB = nil
	return
}

func (fl *TPCCFollower) CloseConnections() (err error) {
	for i := 0; i < len(fl.dbconnections); i++ {
		err = closeDB(fl.dbconnections[i])
	}
	return
}

func OpenDB(tpccConfig Config) (*sql.DB, error) {
	var (
		tmpDB *sql.DB
		err   error
	)
	clientDB, err = newDB(tpccConfig.Targets, tpccConfig.Driver, tpccConfig.User, tpccConfig.Password, tpccConfig.DBName, tpccConfig.ConnParams)
	if err != nil {
		panic(err)
	}
	if err := clientDB.Ping(); err != nil {
		if isDBNotExist(tpccConfig, err) {
			tmpDB, _ = newDB(tpccConfig.Targets, tpccConfig.Driver, tpccConfig.User, tpccConfig.Password, "", tpccConfig.ConnParams)
			defer tmpDB.Close()
			if _, err := tmpDB.Exec(createDBDDL + tpccConfig.DBName); err != nil {
				panic(fmt.Errorf("failed to create database, err %v\n", err))
			}
		} else {
			clientDB = nil
		}
	} else {
		clientDB.SetMaxIdleConns(tpccConfig.Threads + tpccConfig.AcThreads + 1) //TODO Check what to do with each client
	}

	return clientDB, err
}

func isDBNotExist(tpccConfig Config, err error) bool {
	if err == nil {
		return false
	}
	switch tpccConfig.Driver {
	case mysqlDriver:
		return strings.Contains(err.Error(), "Unknown database")
	case pgDriver:
		msg := err.Error()
		return strings.HasPrefix(msg, "pq: database") && strings.HasSuffix(msg, "does not exist")
	}
	return false
}

func (fl *TPCCFollower) TpccService(args TpccArgs, metrics *[]map[string]string) {

	//Set configuration parameters sent from the leader
	TpccConfig = args.TpccConfig
	//TpccConfig.Action = args.TpccConfig.Action
	TpccConfig.Threads = fl.clientThreadNum
	//TpccConfig.TotalCount = args.TpccConfig.TotalCount
	//TpccConfig.MaxProcs = args.TpccConfig.MaxProcs
	//TpccConfig.TotalTime = args.TpccConfig.TotalTime
	//TpccConfig.OutputInterval = args.TpccConfig.OutputInterval

	if len(TpccConfig.Targets) == 0 {
		TpccConfig.Targets = makeTargets(TpccConfig.Hosts, TpccConfig.Ports)
	}

	Transactions = args.Transactions
	Seeds = args.Seeds
	Results = make([]map[string]string, 0, 6)

	switch TpccConfig.Action {
	case "prepare":
		executeTpcc("prepare", fl.dbconnections)
	case "run":
		executeTpcc("run", fl.dbconnections)
	case "cleanup":
		executeTpcc("cleanup", fl.dbconnections)
	case "check":
		executeTpcc("check", fl.dbconnections)
	default:
		err := errors.New("unidentified Action")
		log.Errorf("err: %v | received action: %v", err, TpccConfig.Action)
	}

	*metrics = append(Results, *metrics...)

}

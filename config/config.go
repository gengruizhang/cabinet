package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
)

const (
	ServerID = iota
	ServerIP
	ServerRPCListenerPort
)

func ParseClusterConfig(numOfServers int, path string) (info [][]string) {

	var fileRows []string

	s, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := s.Close()
		if err != nil {
			panic(err)
		}
	}()

	scanner := bufio.NewScanner(s)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		fileRows = append(fileRows, scanner.Text())
	}

	if len(fileRows) < numOfServers {
		err := fmt.Sprintf("insufficient configs for servers | # rows: %v | # servers: %v", len(fileRows), numOfServers)
		panic(errors.New(err))
	}

	for i := 0; i < len(fileRows); i++ {
		row := strings.Split(fileRows[i], " ")
		info = append(info, row)
	}

	return info
}

func ParseThresholds(path string) (possibleTs chan int) {
	var fileRows []string

	possibleTs = make(chan int, 100)

	s, err := os.Open(path)
	if err != nil {
		panic(err)
	}

	defer func() {
		err := s.Close()
		if err != nil {
			panic(err)
		}
	}()

	scanner := bufio.NewScanner(s)
	scanner.Split(bufio.ScanLines)

	for scanner.Scan() {
		fileRows = append(fileRows, scanner.Text())
	}

	if len(fileRows) > 1 {
		err := fmt.Sprintf("this file is supposed to have one row | got %d rows", len(fileRows))
		panic(errors.New(err))
	}

	row := strings.Split(fileRows[0], " ")

	for _, s := range row {
		v, err := strconv.Atoi(s)
		if err != nil {
			panic(err)
		}
		//possibleTs = append(possibleTs, v)
		possibleTs <- v
	}

	return possibleTs
}

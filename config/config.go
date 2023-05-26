package config

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"
)

const (
	ServerID = iota
	ServerIP
	ServerRPCListenerPort
)

func Parser(numOfServers int, path string) (info [][]string) {

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

package mongodb

import (
	"bufio"
	"fmt"
	"os"
	"strings"
)

type Query struct {
	Op     int
	Table  string
	Key    string
	Values map[string]string
}

const (
	INSERT = iota + 3
	READ
	UPDATE
	SCAN
	DELETE
	DROP
)

func lineToQuery(ln string, query *Query) error {
	params := strings.SplitN(ln, " ", 4)
	query.Table, query.Key = params[1], params[2]
	rawValue := params[3]

	curValue := make(map[string]string)

	switch params[0] {
	case "INSERT":
		query.Op = INSERT
		rawFields := strings.Split(rawValue[1:len(rawValue)-2], " field")
		for _, rawField := range rawFields {
			if rawField == "" {
				continue
			}
			rawVal := strings.SplitN(rawField, "=", 2)
			curField := "field" + rawVal[0]
			curValue[curField] = rawVal[1]
		}
	case "READ":
		query.Op = READ
		curValue["<all fields>"] = ""
	case "UPDATE":
		query.Op = UPDATE
		rawField := strings.SplitN(rawValue[2:len(rawValue)-2], "=", 2)
		curValue[rawField[0]] = rawField[1]
	case "SCAN":
		query.Op = SCAN
		scanCount := strings.SplitN(params[3], " ", 2)
		curValue["<all fields>"] = scanCount[0]
	case "DELETE":
		query.Op = DELETE
		curValue["<all fields>"] = ""
	default:
		return fmt.Errorf("Unexpected operator: %s\n", params[0])
	}
	query.Values = curValue

	return nil
}

func ReadQueryFromFile(fn string) (queries []Query, err error) {
	f, err := os.Open(fn)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	fs := bufio.NewScanner(f)
	fs.Split(bufio.ScanLines)

	for fs.Scan() {
		ln := fs.Text()
		var query Query
		if strings.HasPrefix(ln, "INSERT") ||
			strings.HasPrefix(ln, "READ") ||
			strings.HasPrefix(ln, "UPDATE") ||
			strings.HasPrefix(ln, "SCAN") ||
			strings.HasPrefix(ln, "DELETE") {
			if err = lineToQuery(ln, &query); err != nil {
				return nil, err
			}
		} else {
			continue
		}

		queries = append(queries, query)
	}

	return queries, nil
}

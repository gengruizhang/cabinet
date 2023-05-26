package config

import "testing"

func TestParser(t *testing.T) {
	info := Parser(3, "./cluster_localhost.conf")
	t.Logf("%+v\n", info)
}

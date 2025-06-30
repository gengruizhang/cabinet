package config

import "testing"

func TestParseClusterConfig(t *testing.T) {
	info := ParseClusterConfig(10, "./cluster_localhost.conf")
	t.Logf("%+v\n", info)
}

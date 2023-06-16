package eval

import (
	"testing"
	"time"
)

func TestPerfMeter_Init(t *testing.T) {
	var perfM PerfMeter

	perfM.Init(1, 10, "n10_f3_b100_edward")
}

func TestPerfMeter_SaveToFile(t *testing.T) {
	var perfM PerfMeter

	perfM.Init(1, 10000, "test_n10_f3_b100_edward")

	perfM.RecordStarter(1)
	time.Sleep(1589 * time.Millisecond)

	err := perfM.RecordFinisher(1)
	if err != nil {
		t.Error(err)
		return
	}

	perfM.RecordStarter(2)
	time.Sleep(2256 * time.Millisecond)

	err = perfM.RecordFinisher(2)
	if err != nil {
		t.Error(err)
		return
	}

	perfM.RecordStarter(3)

	err = perfM.SaveToFile()
	if err != nil {
		t.Error(err)
		return
	}
}

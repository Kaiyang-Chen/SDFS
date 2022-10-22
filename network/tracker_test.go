package network

import (
	"testing"
	"time"
)

func TestPeriodicalRecorder(t *testing.T) {
	go PeriodicalRecorder()
	time.Sleep(400 * time.Millisecond)
	RecordReceivedPacket(92)
	time.Sleep(600 * time.Millisecond)
	RecordSentPacket(14)
	time.Sleep(200 * time.Millisecond)
	RecordReceivedPacket(18)
	time.Sleep(1200 * time.Millisecond)
	RecordSentPacket(66)
	time.Sleep(400 * time.Millisecond)
	StopRecording()
	// TODO: test the dumped hist data
}

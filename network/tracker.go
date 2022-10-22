package network

import (
	"CS425MP2/config"
	"fmt"
	"log"
	"sync"
	"time"
)

type TrackerEntry struct {
	CurrentTime       string
	BandwidthReceived int
	BandwidthSent     int
}

type Tracker struct {
	Done                   chan bool
	TotalBandwidthReceived int
	TotalBandwidthSent     int
	BandwidthHist          []TrackerEntry
	Mu                     sync.Mutex
}

var MyTracker = Tracker{
	make(chan bool),
	0,
	0,
	make([]TrackerEntry, 0),
	sync.Mutex{},
}

func RecordReceivedPacket(n int) {
	MyTracker.Mu.Lock()
	defer MyTracker.Mu.Unlock()
	MyTracker.TotalBandwidthReceived += n
}

func RecordSentPacket(n int) {
	MyTracker.Mu.Lock()
	defer MyTracker.Mu.Unlock()
	MyTracker.TotalBandwidthSent += n
}

func StopRecording() {
	MyTracker.Done <- true
}

func DumpRecord() {
	fmt.Printf("[Server%d]: MyTracker=%v\n\n", config.MyConfig.ServerID, MyTracker)
	// TODO: dump the record time and bandwidth to a csv file, which will be used for plotting
}

// PeriodicalRecorder
// Need to start a new goroutine to run this function
func PeriodicalRecorder() {
	ticker := time.NewTicker(200 * time.Second)
	defer ticker.Stop()
	defer DumpRecord()

	for {
		select {
		case <-MyTracker.Done:
			return
		case t := <-ticker.C:
			MyTracker.Mu.Lock()
			MyTracker.BandwidthHist = append(MyTracker.BandwidthHist, TrackerEntry{
				t.String(),
				MyTracker.TotalBandwidthReceived,
				MyTracker.TotalBandwidthSent,
			})
			log.Printf("[Server%d at time %v]: sent %d bytes, received %d bytes\n",
				config.MyConfig.ServerID, t, MyTracker.TotalBandwidthSent, MyTracker.TotalBandwidthReceived)
			MyTracker.Mu.Unlock()
		}
	}
}

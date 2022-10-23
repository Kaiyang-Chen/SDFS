package main

import (
	"CS425MP2/SWIM"
	"CS425MP2/config"
	"CS425MP2/io"
	"log"
	"os"
	// "CS425MP2/network"
)
func init() {
	f, err := os.OpenFile("./sdfs.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		return
	}
	log.SetOutput(f)
}

// Parameter should be passed using format like "-ServerID 0 -Address 127.0.0.1:8880"
func main() {
	// flag.StringVar(&config.MyConfig.IP, "IP", "127.0.0.1", "The IP for the current process")
	// flag.StringVar(&config.MyConfig.Port, "Port", "8880", "The UDP port for the current process")
	// flag.Parse()
	config.InitConfig()

	SWIM.InitSwimInstance()
	io.Handle_IO()


	// go network.PeriodicalRecorder()

	// for {
	// 	time.Sleep(1000 * time.Millisecond)
	// 	SWIM.MySwimInstance.SwimShowPeer()
	// }
}

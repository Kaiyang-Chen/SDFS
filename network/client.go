package network

import (
	"errors"
	"log"
	"math/rand"
	"net"
)

const DropPacketProbability = 0.00

func tossCoin() bool {
	if rand.Float64() >= DropPacketProbability {
		return true
	}
	return false
}

const MAXCONN = 1024

var CONN = 0

func Dial(host string, request []byte) ([]byte, error) {
	if CONN >= MAXCONN {
		return nil, errors.New("maximum connection reached")
	}
	CONN++
	udpAddr, err := net.ResolveUDPAddr("udp", host)
	connection, err := net.DialUDP("udp", nil, udpAddr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer connection.Close()

	n, err := connection.Write(request)
	RecordSentPacket(n)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	buffer := make([]byte, 1024)
	n, _, err = connection.ReadFromUDP(buffer)
	RecordReceivedPacket(n)
	if !tossCoin() {
		return nil, net.UnknownNetworkError("Packet dropped")
	}
	if err != nil {
		log.Println(err)
		return nil, err
	}
	CONN--
	return buffer[:n], nil
}

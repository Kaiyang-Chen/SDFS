package network

import (
	"log"
	"net"
)

func Listen(address string, messageHandler func([]byte) []byte) error {
	udpAddr, err := net.ResolveUDPAddr("udp", address)
	if err != nil {
		log.Println(err)
		return err
	}

	connection, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Println(err)
		return err
	}

	defer connection.Close()
	buffer := make([]byte, 1024)

	for {
		n, addr, err := connection.ReadFromUDP(buffer)
		if err != nil {
			log.Println(err)
			return err
		} else {
			RecordReceivedPacket(n)
		}
		bufferCopy := make([]byte, n)
		copy(bufferCopy, buffer[:n])
		go func(packet []byte) {
			response := messageHandler(packet)
			n, err := connection.WriteToUDP(response, addr)
			if err != nil {
				log.Println(err)
				return
			} else {
				RecordSentPacket(n)
			}
		}(bufferCopy)
	}
}

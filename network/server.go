package network

import (
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
)

func Listen(address string, messageHandler func([]byte) (string, []byte)) error {
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
		fmt.Println(connection.RemoteAddr())
		if err != nil {
			log.Println(err)
			return err
		} else {
			RecordReceivedPacket(n)
		}
		bufferCopy := make([]byte, n)
		copy(bufferCopy, buffer[:n])
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func(packet []byte) {
			// Notice, for sdfs, the message handler will return file path
			filePath, response := messageHandler(packet)
			if len(filePath) > 0 {
				fmt.Printf("receive header\n")
				_, err = connection.WriteToUDP([]byte("ok"), addr)
				if err != nil {
					fmt.Println(err)
					log.Println(err)
					return
				}
			} else {
				_, err = connection.WriteToUDP(response, addr)
				wg.Done()
			}
			if err != nil {
				log.Println(err)
				return
			} else {
				RecordSentPacket(n)
			}
			if len(filePath) > 0 {
				RecvFile(filePath, connection)
				wg.Done()
			}

		}(bufferCopy)
		wg.Wait()
	}

}

func RecvFile(fileName string, conn *net.UDPConn) {
	if conn == nil {
		fmt.Printf("null pointer")
		return
	}
	fmt.Println("creating file: ", fileName)
	f, err := os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0755)
	// f, err := os.Create(fileName)
	if err != nil {
		fmt.Println("Create err:", err)
		log.Println("Create err:", err)
		return
	}
	defer f.Close()

	fmt.Println(conn.RemoteAddr())
	fmt.Println("start receiving file: ", fileName)
	buf := make([]byte, 4096)
	for {
		fmt.Printf("aa")
		n, addr, err := conn.ReadFromUDP(buf)
		fmt.Println(addr)
		// TODO: need to handle EOF
		if err != nil {
			if err == io.EOF {
				fmt.Println("File received: ", fileName)
				log.Println("File received: ", fileName)
				_, err := conn.WriteToUDP([]byte("received"), addr)
				if err != nil {
					log.Println(err)
				}
			} else {
				log.Println("Read file err:", err)
			}
			return
		}
		f.Write(buf[:n])
	}
}

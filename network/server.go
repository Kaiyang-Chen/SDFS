package network

import (
	"fmt"
	"log"
	"net"
	"os"
	"sync"
	"bufio"
)

func Listen(address string, messageHandler func([]byte) (bool, string, []byte)) error {
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
		// fmt.Println(connection.RemoteAddr())
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
			flag, filePath, response := messageHandler(packet)
			if len(filePath) > 0 {
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
				RecvFile(filePath, connection, flag)
				wg.Done()
			}

		}(bufferCopy)
		wg.Wait()
	}

}

func RecvFile(fileName string, conn *net.UDPConn, flag bool) {
	var f *os.File
	var err error
	if flag {
		f, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		write := bufio.NewWriter(f)
		write.WriteString("--------------------------------------------------------- \n")
	}else{
		f, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0755)
	}
	// f, err := os.Create(fileName)
	if err != nil {
		log.Println("Create err:", err)
		return
	}
	defer f.Close()

	// fmt.Println(conn.RemoteAddr())
	fmt.Println("start receiving file: ", fileName)
	buf := make([]byte, 4096)
	for {
		n, addr, err := conn.ReadFromUDP(buf)
		if err != nil || n == 0 {
			if n == 0 {
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

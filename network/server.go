package network

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
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
	buffer := make([]byte, 8096)

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
			_, filePath, response := messageHandler(packet)
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
				// RecvFile(filePath, connection, flag)
				wg.Done()
			}

		}(bufferCopy)
		wg.Wait()
	}

}

func ListenTcp(address string, messageHandler func([]byte) (bool, string, []byte)) error {
	// udpAddr, err := net.ResolveUDPAddr("udp", address)
	// if err != nil {
	// 	log.Println(err)
	// 	return err
	// }
	tcpAddr, err := net.ResolveTCPAddr("tcp", address)
	connection, err := net.ListenTCP("tcp", tcpAddr)
	if err != nil {
		log.Println(err)
		return err
	}

	defer connection.Close()
	buffer := make([]byte, 8096)

	for {
		// n, addr, err := connection.ReadFromUDP(buffer)
		conn, err := connection.AcceptTCP()
		// fmt.Println(connection.RemoteAddr())
		if err != nil {
			log.Println(err)
			return err
		}
		n, err := conn.Read(buffer)
		bufferCopy := make([]byte, n)
		copy(bufferCopy, buffer[:n])
		wg := sync.WaitGroup{}
		wg.Add(1)
		go func(packet []byte) {
			// Notice, for sdfs, the message handler will return file path
			flag, filePath, response := messageHandler(packet)
			if len(filePath) > 0 {
				_, err = conn.Write([]byte("ok"))
				if err != nil {
					fmt.Println(err)
					log.Println(err)
					return
				}
			} else {
				_, err = conn.Write(response)
				wg.Done()
			}
			if err != nil {
				log.Println(err)
				return
			}
			if len(filePath) > 0 {
				RecvFile(filePath, conn, flag)
				wg.Done()
			}

		}(bufferCopy)
		wg.Wait()
	}

}

func RecvFile(fileName string, conn *net.TCPConn, flag bool) {
	var f *os.File
	var err error
	if flag {
		f, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755)
		write := bufio.NewWriter(f)
		write.WriteString("--------------------------------------------------------- \n")
		write.Flush()
	} else {
		f, err = os.OpenFile(fileName, os.O_WRONLY|os.O_CREATE, 0755)
	}
	// f, err := os.Create(fileName)
	if err != nil {
		log.Println("Create err:", err)
		return
	}
	defer f.Close()

	// fmt.Println(conn.RemoteAddr())
	// fmt.Println("start receiving file: ", fileName)
	buf := make([]byte, 4096)
	for {
		n, err := conn.Read(buf)
		// fmt.Println(n)
		if err != nil || n == 0 {
			if n == 0 || err == io.EOF {
				// fmt.Println("File received: ", fileName)
				log.Println("File received: ", fileName)
				// _, err := conn.Write([]byte("received"))
				// if err != nil {
				// 	log.Println(err)
				// }
			} else {
				log.Println("Read file err:", err)
			}
			return
		}
		f.Write(buf[:n])
	}
}

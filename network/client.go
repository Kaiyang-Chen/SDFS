package network

import (
	"errors"
	"fmt"
	// "io"
	"log"
	"math/rand"
	"net"
	"os"
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

func SendFile(path string, conn *net.TCPConn) ([]byte, int, error) {
	f, err := os.Open(path)
	if err != nil {
		log.Println("os.Open err:", err)
		return nil, 0, err
	}
	defer f.Close()
	// fmt.Println("start sending file: ", path)
	log.Println("start sending file: ", path)
	// send all contents in file
	buf := make([]byte, 4096)
	for {
		n, err := f.Read(buf)
		fmt.Println(n)
		if err != nil || n == 0{
			if n == 0 {
				log.Println("file read done: ", path)
				conn.Write(buf[:n])
				break
			} else {
				log.Println("f.Read err:", err)
				return nil, 0, err
			}

		}
		conn.Write(buf[:n])
	}

	// fmt.Println("end sending file: ", path)
	// Check whether file transmission done on server side
	buffer := make([]byte, 1024)
	// n, err := conn.Read(buffer)
	// if err != nil {
	// 	log.Println(err)
	// 	return nil, 0, err
	// }
	return buffer, 0, nil
}

// func SdfsDial(host string, FilePath string, sdfsName string, request []byte) ([]byte, error) {
// 	if CONN >= MAXCONN {
// 		return nil, errors.New("maximum connection reached")
// 	}
// 	CONN++
// 	udpAddr, err := net.ResolveUDPAddr("udp", host)
// 	connection, err := net.DialUDP("udp", nil, udpAddr)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}

// 	defer connection.Close()

// 	n, err := connection.Write(request)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	buffer := make([]byte, 1024)
// 	n, _, err = connection.ReadFromUDP(buffer)
// 	if err != nil {
// 		log.Println(err)
// 		return nil, err
// 	}
// 	if(len(FilePath) != 0 && len(sdfsName) != 0){
// 		if "ok" == string(buffer[:n]) {
// 			buffer, n, err = SendFile(FilePath, connection)
// 		}
// 		CONN--
// 		if "received" == string(buffer[:n]) {
// 			fmt.Println("File sending succeed: ", FilePath)
// 			log.Println("File sending failed: ", FilePath)
// 			return buffer[:n], nil
// 		} else {
// 			log.Println("File sending failed: ", FilePath)
// 			return buffer[:n], errors.New("File sending failed.")
// 		}
// 	} else{
// 		CONN--
// 		return buffer[:n], nil
// 	}
// }
func SdfsDial(host string, FilePath string, sdfsName string, request []byte) ([]byte, error) {
	if CONN >= MAXCONN {
		return nil, errors.New("maximum connection reached")
	}
	CONN++
	tcpAddr, err := net.ResolveTCPAddr("tcp", host)
	connection, err := net.DialTCP("tcp", nil, tcpAddr)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	defer connection.Close()

	n, err := connection.Write(request)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	buffer := make([]byte, 1024)
	n, err = connection.Read(buffer)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	if len(FilePath) != 0 && len(sdfsName) != 0 {
		if "ok" == string(buffer[:n]) {
			buffer, n, err = SendFile(FilePath, connection)
		}
		CONN--
		return buffer[:n], nil
		// if "received" == string(buffer[:n]) {
		// 	fmt.Println("File sending succeed: ", FilePath)
		// 	log.Println("File sending failed: ", FilePath)
		// 	return buffer[:n], nil
		// } else {
		// 	log.Println("File sending failed: ", FilePath)
		// 	return buffer[:n], errors.New("File sending failed.")
		// }
	} else {
		CONN--
		return buffer[:n], nil
	}
}

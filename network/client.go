package network

import (
	"errors"
	"log"
	"math/rand"
	"net"
	"os"
	"io"
	"fmt"
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

func SendFile(path string, conn* net.UDPConn) ([]byte, int, error) {
	f, err := os.Open(path)
	if err != nil {
	   log.Println("os.Open err:", err)
	   return nil, 0, err
	}
	defer f.Close()                 
 
	// send all contents in file
	buf := make([]byte, 4096)
	for {
	   	n, err := f.Read(buf)        
	   	if err != nil {
		  	if err == io.EOF {
			 	log.Println("file read done: ", path)
			 	break
		  	} else {
			 	log.Println("f.Read err:", err)
			 	return nil, 0, err
		  	}
		  
	   }
	   conn.Write(buf[:n]) 
	}
	// Check whether file transmission done on server side
	n, _, err := conn.ReadFromUDP(buf)
	if err != nil {
		log.Println(err)
		return nil, 0, err
	}
	return buf, n, nil
}
 

func SdfsDial(host string, FilePath string) ([]byte, error) {
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
	fileInfo, err := os.Stat(FilePath)
	if err != nil {
		log.Println("os.Stat err:", err)
		return nil, err
	}

	n, err := connection.Write([]byte(fileInfo.Name()))
	if err != nil {
		log.Println(err)
		return nil, err
	}
	buffer := make([]byte, 1024)
	fmt.Printf("test1 \n")
	n, _, err = connection.ReadFromUDP(buffer)
	fmt.Printf("test2 \n")
	if err != nil {
		fmt.Println(err)
		log.Println(err)
		return nil, err
	}
	fmt.Println(buffer[:n])
	if "ok" == string(buffer[:n]) {
		fmt.Printf("begin sending file \n")
		buffer, n, err = SendFile(FilePath, connection)   
	}
	CONN--
	if "received" == string(buffer[:n]) {
		fmt.Printf("end sending file \n")
		return buffer[:n], nil
	} else {
		log.Println("File sending failed: ", FilePath)
		return buffer[:n], errors.New("File sending failed.")
	}
	
}

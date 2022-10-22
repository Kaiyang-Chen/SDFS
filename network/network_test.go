package network

import (
	"CS425MP2/config"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
)

type Member struct {
	State int // One of WORKING, MAYBEDEAD or DEAD
}

type SWIM struct {
	MembershipList   []Member
	PiggybackBuffer  []Piggyback
	MyIncarnationNum int
}

type Piggyback struct {
	Type                 int    // One of SUSPECT, ALIVE or CONFIRM
	ServerAddr           string // Which server this piggyback message refers to
	ServerIncarnationNum int    // Incremental message ID for this server
}

type Message struct {
	SenderAddr string // Sender's IP + Port

	MessageType int         // One of PING, PINGREQ or ACK
	Piggybacks  []Piggyback // Messages that need to be piggybacked

	NewConfig *config.Config // If there's new node joining the group, we need to update all nodes' config
}

func TestNetworkWithMarshalA(t *testing.T) {
	testHandler := func(request []byte) []byte {
		var message Message
		err := json.Unmarshal(request, &message)
		if err != nil {
			return nil
		}
		fmt.Printf("Request is %v\n", message)
		return []byte("This is a test respond")
	}
	go Listen("127.0.0.1:8888", testHandler)
	data := Message{
		SenderAddr:  "127.0.0.1:8888",
		MessageType: 0,
		Piggybacks: []Piggyback{
			{1, "127.0.0.1:8888", 0},
			{2, "127.0.0.1:8888", 0},
		},
		NewConfig: nil,
	}
	jsonData, _ := json.Marshal(data)
	respond, err := Dial("127.0.0.1:8888", jsonData)
	if err != nil {
		return
	}
	fmt.Printf("Respond is %v\n", string(respond))
}

func TestNetworkWithMarshalB(t *testing.T) {
	testHandler := func(request []byte) []byte {
		var message Message
		err := json.Unmarshal(request, &message)
		if err != nil {
			return nil
		}
		fmt.Printf("Request is %v\n", message)
		return []byte("This is a test respond")
	}
	go Listen("127.0.0.1:8888", testHandler)
	data := Message{
		SenderAddr:  "127.0.0.1:8888",
		MessageType: 0,
		Piggybacks: []Piggyback{
			{1, "127.0.0.1:8888", 0},
			{2, "127.0.0.1:8888", 0},
		},
		NewConfig: &config.Config{
			0, "127.0.0.1", "8888", nil, "127.0.0.1:8000", sync.RWMutex{},
		},
	}
	jsonData, _ := json.Marshal(data)
	respond, err := Dial("127.0.0.1:8888", jsonData)
	if err != nil {
		return
	}
	fmt.Printf("Respond is %v\n", string(respond))
	fmt.Printf("NewConfig is %v\n", string(respond))
}

func TestNetworkFailure(t *testing.T) {
	data := Message{
		SenderAddr:  "127.0.0.1:8888",
		MessageType: 0,
		Piggybacks: []Piggyback{
			{1, "127.0.0.1:8888", 0},
			{2, "127.0.0.1:8888", 0},
		},
		NewConfig: &config.Config{
			0, "127.0.0.1", "8888", nil, "127.0.0.1:8000", sync.RWMutex{},
		},
	}
	jsonData, _ := json.Marshal(data)
	_, err := Dial("127.0.0.1:8888", jsonData)
	if err != nil {
		fmt.Println(err)
		return
	}
}

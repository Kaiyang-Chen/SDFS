package SWIM

import (
	"CS425MP2/config"
	"CS425MP2/network"
	"bufio"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

// Types for message
const (
	PING      = 0
	PINGREQ   = 1
	ACK       = 2
	INTRODUCE = 3 // You should go to the introducer to add you into the group
	JOIN      = 4 // Ask introducer to synchronize its config
)

// Types for piggyback / states of a member
const (
	ALIVE   = 0
	SUSPECT = 1
	CONFIRM = 2
)

type Member struct {
	State int // One of WORKING, MAYBEDEAD or DEAD
}

type Piggyback struct {
	Type                 int    // One of SUSPECT, ALIVE or CONFIRM
	ServerAddr           string // Which server this piggyback message refers to
	ServerIncarnationNum int    // Incremental message ID for this server
}

type Message struct {
	SenderAddr string // Sender's IP + Port, to distinguish if this is a new node

	MessageType int         // One of PING, PINGREQ or ACK
	PingTarget  string      // Only for PINGREQ, the server I want you to ping; empty string for other types of message
	Piggybacks  []Piggyback // Messages that need to be piggybacked

	// If there's new node joining the group, we need to update all nodes' config
	// If no new node joining, then nil is passed to this field, so we don't need to update anything
	NewConfig *config.Config
}

type SWIM struct {
	MembershipList        map[string]*Member   // Address -> Member; in the beginning, assume all members are alive
	PiggybackBuffer       map[Piggyback]int    // Piggyback payload -> how many times they are sent
	MyIncarnationNum      int                  // Incarnation number to distinguish multiply messages
	ServerNewestPiggyback map[string]Piggyback // Server address -> newest received piggyback for that server
	Mu                    sync.RWMutex
}

var MySwimInstance SWIM

// GetPiggybacks
// Delete piggybacks that has been sent successfully more than 4 times, and return the rest available piggybacks
// This function will not lock swim instance, so caller should be responsible for that
func (swim *SWIM) GetPiggybacks() []Piggyback {
	swim.Mu.Lock()
	defer swim.Mu.Unlock()
	availablePiggybacks := make([]Piggyback, 0)
	for piggyback, sendCount := range swim.PiggybackBuffer {
		if sendCount >= 10 {
			delete(swim.PiggybackBuffer, piggyback)
		} else {
			availablePiggybacks = append(availablePiggybacks, piggyback)
		}
	}
	return availablePiggybacks
}

func (swim *SWIM) UpdateMembershipList() {
	swim.Mu.Lock()
	PeersAddr := config.MyConfig.GetPeersAddr()
	for _, peer := range PeersAddr {
		swim.MembershipList[peer] = &Member{ALIVE}
	}
	swim.Mu.Unlock()
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func (swim *SWIM) JoinViaIntroducer() {
	if !config.MyConfig.IsIntroducer() {
		for {
			// Introducer will send PING to me to update my peer address, so I don't need to handle the reply here
			_, err := swim.SendJOIN()
			if err != nil {
				log.Printf("PING Introducer failed: %v\n", err)
				time.Sleep(100 * time.Millisecond)
			} else {
				break
			}
		}
	}
}

func convertStateToStr(state int) string {
	var status string
	if state == ALIVE {
		status = "ALIVE"
	} else if state == SUSPECT {
		status = "SUSPECT"
	} else if state == CONFIRM {
		status = "CONFIRM"
	}
	return status
}

func (swim *SWIM) SwimShowPeer() {
	log.Printf("[Server %s]: membership list:\n", config.MyConfig.GetMyAddr())
	for addr, member := range swim.MembershipList {
		status := convertStateToStr(member.State)
		fmt.Printf("[Peer %s]: %s\n", addr, status)
	}
	fmt.Printf("\n")
}

func InitSwimInstance() {
	MySwimInstance.MembershipList = make(map[string]*Member)
	for _, addr := range config.MyConfig.PeersAddr {
		MySwimInstance.MembershipList[addr] = &Member{ALIVE}
	}
	MySwimInstance.PiggybackBuffer = make(map[Piggyback]int)
	MySwimInstance.MyIncarnationNum = 0
	MySwimInstance.ServerNewestPiggyback = make(map[string]Piggyback)

	// I'm not the introducer, so I need to ping the introducer, and it will send me a new config to update
	// If no ACK received, just keep pinging
	MySwimInstance.JoinViaIntroducer()

	// Let this SWIM instance listening to any incoming request
	go func() {
		err := network.Listen(config.MyConfig.GetMyAddr(), HandleMessage)
		if err != nil {
			log.Fatal(err)
		}
	}()

	go MySwimInstance.periodicalPing()
	inputReader := bufio.NewReader(os.Stdin)
	for {
		input, err := inputReader.ReadString('\n')
		input = strings.Replace(input, "\n", "", -1)
		if err != nil || input == "" {
			fmt.Printf("Failed to read the input! Try again!\n")
			continue
		}

		if strings.Compare("leave", input) == 0 {
			if !config.MyConfig.IsIntroducer() {
				os.Exit(0)
			}
		}

	}
}

func (swim *SWIM) periodicalPing() {
	for {
		var myIndex int
		config.MyConfig.Mu.RLock()
		for i, v := range config.MyConfig.PeersAddr {
			if v == config.MyConfig.GetMyAddr() {
				myIndex = i
				break
			}
		}
		for i := 0; i < 4; i++ {
			peerIndex := (myIndex + i + 1) % len(config.MyConfig.PeersAddr)
			proxyAddr := config.MyConfig.PeersAddr[peerIndex]
			if proxyAddr == config.MyConfig.GetMyAddr() {
				continue
			}
			if member, ok := swim.MembershipList[proxyAddr]; !ok || member.State != ALIVE { // Only ping alive servers
				continue
			}
			go func(i int) {
				_, err := MySwimInstance.SendPING(proxyAddr, nil, pingFailureHandler)
				if err != nil {
					log.Println(err)
				}
			}(i)
		}
		config.MyConfig.Mu.RUnlock()
		time.Sleep(500 * time.Millisecond)
	}
}

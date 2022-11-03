package Sdfs

import (
	"CS425MP2/config"
	"encoding/json"
	// "CS425MP2/sdfs"
	"log"
	"time"
	"strings"
)

func HandleMessage(request []byte) (bool, string, []byte) {
	var message Message
	err := json.Unmarshal(request, &message)
	if err != nil {
		log.Println("[Unmarshal]: ", err)
	}
	// fmt.Printf("Request is %v\n", message)
	var reply Message

	// If I haven't seen your address and I'm not the introducer, then you are a new node (fail-recover node),
	// so you should go to the introducer to introduce you into the group
	if _, ok := MySwimInstance.MembershipList[message.SenderAddr]; !ok && !config.MyConfig.IsIntroducer() {
		reply.MessageType = INTRODUCE
		jsonReply, err := json.Marshal(reply)
		if err != nil {
			log.Println("[Marshal]: ", err)
		}
		return false, "", jsonReply
	}

	if message.MessageType == PING {
		reply, err = MySwimInstance.HandlePING(message)
	} else if message.MessageType == PINGREQ {
		reply, err = MySwimInstance.HandlePINGREQ(message)
	} else if message.MessageType == ACK {
		panic("No one is supposed to send ACK message actively!")
	} else if message.MessageType == JOIN {
		reply, err = MySwimInstance.HandleJOIN(message)
	} else {
		panic("Unrecognized message type!")
	}
	jsonReply, err := json.Marshal(reply)
	if err != nil {
		log.Println("[Marshal]: ", err)
	}
	return false, "", jsonReply
}

func (swim *SWIM) HandleJOIN(message Message) (Message, error) {
	log.Printf("[HandleJOIN]: message=%v", message)

	config.MyConfig.Mu.Lock()
	swim.Mu.Lock()
	defer config.MyConfig.Mu.Unlock()
	defer swim.Mu.Unlock()

	// Add the new node into my config and membership list
	SdfsClient.HandleJoin(message.SenderAddr)
	config.MyConfig.PeersAddr = append(config.MyConfig.PeersAddr, message.SenderAddr)
	swim.MembershipList[message.SenderAddr] = &Member{ALIVE}
	go swim.syncNewPeer()

	reply := Message{
		SenderAddr:  config.MyConfig.GetMyAddr(),
		MessageType: ACK,
		PingTarget:  "",
		Piggybacks:  nil,
		NewConfig:   &config.MyConfig,
	}
	return reply, nil
}

func (swim *SWIM) syncNewPeer() {
	config.MyConfig.Mu.RLock()
	defer config.MyConfig.Mu.RUnlock()
	// Send a ping message to all nodes including this new one to update their peer address list
	for _, peer := range config.MyConfig.PeersAddr {
		peer := peer
		var updatePeer func(string)
		updatePeer = func(peerAddr string) {
			// This message only aims to update the peer address, so we don't need to handle the reply
			_, err := swim.SendPING(peer, &config.MyConfig, nil)
			if err != nil {
				log.Println("[SendPING]: ", err)
				time.Sleep(100 * time.Microsecond)
				// Keep pinging because we must update our peer's address
				go updatePeer(peerAddr)
			}
		}
		go updatePeer(peer)
	}
}

func (swim *SWIM) HandlePING(message Message) (Message, error) {
	log.Printf("[HandlePING]: message=%v", message)
	// First we need to handle the piggyback
	swim.HandlePiggybacks(message.Piggybacks)

	// Handle the peer address update from the introducer
	if message.NewConfig != nil {
		config.MyConfig.UpdateConfig(message.NewConfig)
		swim.UpdateMembershipList()
	}

	var newConfig *config.Config = nil
	// If I'm the introducer, and I haven't seen the sender's address, it means a new node tries to join the group.
	// Then I need to send my config to it in ACK message, and send others my config in PING message
	if config.MyConfig.IsIntroducer() {
		seen := false
		for _, addr := range config.MyConfig.PeersAddr {
			if addr == message.SenderAddr {
				seen = true
			}
		}
		if !seen || swim.MembershipList[message.SenderAddr].State == CONFIRM { // This is a new node or rejoin node
			config.MyConfig.Mu.Lock()
			config.MyConfig.PeersAddr = append(config.MyConfig.PeersAddr, message.SenderAddr)
			// Update my membership list, new member will be initialized to ALIVE by default
			swim.MembershipList[message.SenderAddr] = &Member{ALIVE}
			newConfig = &config.MyConfig
			config.MyConfig.Mu.Unlock()

			go swim.syncNewPeer()

			// Return an ACK to this new node
			reply := Message{
				SenderAddr:  config.MyConfig.GetMyAddr(),
				MessageType: ACK,
				PingTarget:  "",
				Piggybacks:  nil,
				NewConfig:   nil,
			}
			return reply, nil
		}
	}

	// Not a new node joining the group, so we just return an ACK message
	reply := Message{
		SenderAddr:  config.MyConfig.GetMyAddr(),
		MessageType: ACK,
		PingTarget:  "",
		Piggybacks:  swim.GetPiggybacks(),
		NewConfig:   newConfig,
	}
	return reply, nil
}

func (swim *SWIM) HandlePINGREQ(message Message) (Message, error) {
	log.Printf("[HandlePINGREQ]: message=%v", message)
	// First we need to handle the pigyback
	swim.HandlePiggybacks(message.Piggybacks)

	pingTarget := message.PingTarget
	// We only need to forward the ACK back to PINGREQ's sender, so we don't handle the reply
	reply, err := swim.SendPING(pingTarget, nil, nil)
	if err != nil {
		log.Println(err)
	}
	return reply, err
}

func (swim *SWIM) HandlePiggybacks(piggybacks []Piggyback) {
	log.Printf("[HandlePiggybacks]: piggybacks=%v", piggybacks)
	// swim.SwimShowPeer()
	// defer swim.SwimShowPeer()
	if piggybacks == nil {
		return
	}

	// Rules for suspicion mechanism used here are listed in section 4.2 of the SWIM paper
	swim.Mu.Lock()
	defer swim.Mu.Unlock()

	for _, piggyback := range piggybacks {
		if piggyback.ServerAddr == config.MyConfig.GetMyAddr() {
			// I'm suspected, so I need to send ALIVE message to clarify
			if piggyback.Type == SUSPECT {
				// Increment my number when suspected in the current incarnation
				if piggyback.ServerIncarnationNum == swim.MyIncarnationNum {
					swim.MyIncarnationNum++
				}
				piggyback := Piggyback{
					ALIVE, config.MyConfig.GetMyAddr(), swim.MyIncarnationNum,
				}
				swim.PiggybackBuffer[piggyback] = 0
			}
			// I'm confirmed, so everyone now thinks I'm dead, I need to rejoin the group as a new node
			if piggyback.Type == CONFIRM {
				swim.JoinViaIntroducer()
			}
			continue
		}

		// Resolve conflicts with previous piggyback using rules specified in Section 4.2 of the swim paper
		if prevPiggyback, ok := swim.ServerNewestPiggyback[piggyback.ServerAddr]; ok {
			overridePrev := false
			if piggyback.Type == ALIVE && (prevPiggyback.Type == SUSPECT || prevPiggyback.Type == ALIVE) {
				if piggyback.ServerIncarnationNum > prevPiggyback.ServerIncarnationNum {
					overridePrev = true
				}
			}
			if piggyback.Type == SUSPECT && prevPiggyback.Type == SUSPECT {
				if piggyback.ServerIncarnationNum > prevPiggyback.ServerIncarnationNum {
					overridePrev = true
				}
			}
			if piggyback.Type == SUSPECT && prevPiggyback.Type == ALIVE {
				if piggyback.ServerIncarnationNum >= prevPiggyback.ServerIncarnationNum {
					overridePrev = true
				}
			}
			if piggyback.Type == CONFIRM && (prevPiggyback.Type == SUSPECT || prevPiggyback.Type == ALIVE) {
				overridePrev = true
			}
			if !overridePrev {
				continue
			}
		}
		// Update according to this piggyback
		member, ok := swim.MembershipList[piggyback.ServerAddr]
		// Shit is fucked here
		if !ok {
			continue
		}
		if piggyback.Type == CONFIRM {
			config.MyConfig.Mu.Lock()
			deleteDeadServer(piggyback.ServerAddr)
			config.MyConfig.Mu.Unlock()
			if config.MyConfig.IsIntroducer(){
				SdfsClient.HandleLeaving(piggyback.ServerAddr)
			}
			victim := strings.Split(piggyback.ServerAddr, ":")[0] + ":" + "8889"
			if victim == config.MyConfig.GetLeaderAddr() && SdfsClient.IsNextLeader(){
				SdfsClient.HandleNewMaster()
			}
		} else if piggyback.Type == SUSPECT {
			member.State = piggyback.Type
			go confirmTicker(piggyback.ServerAddr)
		} else {
			member.State = piggyback.Type
		}
		// Forward this piggyback to other peers via gossip
		swim.PiggybackBuffer[piggyback] = 0
		swim.ServerNewestPiggyback[piggyback.ServerAddr] = piggyback
	}
}

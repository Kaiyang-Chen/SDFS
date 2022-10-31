package Sdfs

import (
	"CS425MP2/config"
	"CS425MP2/network"
	"encoding/json"
	"log"
	"time"
)

func (swim *SWIM) SendMessage(request Message, host string) (Message, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Println(err)
	}
	response, err := network.Dial(host, jsonData)
	replyMessage := Message{}
	if err == nil {
		err = json.Unmarshal(response, &replyMessage)
	}
	return replyMessage, err
}

// SendJOIN
// Send JOIN message to the introducer, and update my config.
func (swim *SWIM) SendJOIN() (Message, error) {
	message := Message{
		SenderAddr:  config.MyConfig.GetMyAddr(),
		MessageType: JOIN,
		PingTarget:  "",
		Piggybacks:  swim.GetPiggybacks(),
		NewConfig:   nil,
	}
	reply, err := swim.SendMessage(message, config.MyConfig.IntroducerAddr)
	log.Printf("[SendJOIN]: message=%v, peer=%v\n", message, reply.NewConfig.PeersAddr)
	if reply.NewConfig != nil {
		config.MyConfig.UpdateConfig(reply.NewConfig)
		swim.Mu.Lock()
		for _, addr := range reply.NewConfig.PeersAddr {
			swim.MembershipList[addr] = &Member{ALIVE}
		}
		swim.Mu.Unlock()
	} else {
		log.Fatal("JOIN's response doesn't contain the new config")
	}
	return reply, err
}

// SendPING
// Send PING message to targetAddr, and return the reply (ACK message)
// Note that the caller should handle the ACK message!
func (swim *SWIM) SendPING(targetAddr string, newConfig *config.Config,
	failureHandler func(swim *SWIM, targetAddr string)) (Message, error) {
	message := Message{
		SenderAddr:  config.MyConfig.GetMyAddr(),
		MessageType: PING,
		PingTarget:  "",
		Piggybacks:  swim.GetPiggybacks(),
		NewConfig:   newConfig,
	}
	//log.Printf("[SendPING]: message=%v", message)
	reply, err := swim.SendMessage(message, targetAddr)
	if err == nil && reply.MessageType != ACK && reply.MessageType != INTRODUCE {
		panic("Reply of a normal PING message should be ACK or INTRODUCE!")
	}
	if err != nil && failureHandler != nil {
		failureHandler(swim, targetAddr)
	}
	swim.IncrementPiggybackCount(message.Piggybacks)
	swim.HandlePiggybacks(reply.Piggybacks)
	return reply, err
}

// SendPINGREQ
// Ask another node to ping the target node, and forward the ACK reply to the sender
// Use proxyChan to indicate if this PING message succeeds or not
func (swim *SWIM) SendPINGREQ(receiverAddr string, targetAddr string, indirectPingSuccess *chan bool) {
	message := Message{
		SenderAddr:  receiverAddr,
		MessageType: PING,
		PingTarget:  targetAddr,
		Piggybacks:  swim.GetPiggybacks(),
		NewConfig:   nil,
	}
	//log.Printf("[SendPINGREQ]: message=%v", message)
	reply, err := swim.SendMessage(message, targetAddr)
	swim.IncrementPiggybackCount(message.Piggybacks)
	swim.HandlePiggybacks(reply.Piggybacks)
	if err != nil {
		*indirectPingSuccess <- false
	} else {
		*indirectPingSuccess <- true
	}
}

// If ping failed, pick 4 nodes to ping it for me
func pingFailureHandler(swim *SWIM, targetAddr string) {
	// Find my index in peer address
	var myIndex int
	config.MyConfig.Mu.RLock()
	for i, v := range config.MyConfig.PeersAddr {
		if v == config.MyConfig.GetMyAddr() {
			myIndex = i
			break
		}
	}
	indirectPingSuccess := make(chan bool, 4)
	for i := 0; i < 4; i++ {
		peerIndex := (myIndex + i + 1) % len(config.MyConfig.PeersAddr)
		proxyAddr := config.MyConfig.PeersAddr[peerIndex]
		if proxyAddr == config.MyConfig.GetMyAddr() {
			indirectPingSuccess <- false
			continue
		}
		go swim.SendPINGREQ(proxyAddr, targetAddr, &indirectPingSuccess)
	}
	config.MyConfig.Mu.RUnlock()
	alive := false
	for i := 0; i < 4; i++ {
		result := <-indirectPingSuccess
		alive = alive || result
	}
	swim.Mu.Lock()
	if !alive {
		piggyback := Piggyback{
			SUSPECT, targetAddr, swim.ServerNewestPiggyback[targetAddr].ServerIncarnationNum,
		}
		swim.PiggybackBuffer[piggyback] = 0
		member := swim.MembershipList[targetAddr]
		member.State = SUSPECT
		go confirmTicker(targetAddr)
	}
	swim.Mu.Unlock()
}

// confirmTicker
// If a node is suspected for longer than 500ms, and its state hasn't been changed,
// then we can say this node is dead. We reset its state to CONFIRM,
// and add a "CONFIRM" piggyback into our piggyback buffer to disseminate
func confirmTicker(targetAddr string) {
	time.Sleep(500 * time.Millisecond)

	MySwimInstance.Mu.Lock()
	config.MyConfig.Mu.Lock()
	defer MySwimInstance.Mu.Unlock()
	defer config.MyConfig.Mu.Unlock()

	member, ok := MySwimInstance.MembershipList[targetAddr]
	if !ok { // already deleted
		return
	}
	if member.State == SUSPECT {
		member.State = CONFIRM
	}
	deleteDeadServer(targetAddr)
	if config.MyConfig.IsIntroducer(){
		SdfsClient.HandleLeaving(targetAddr)
	}
	piggyback := Piggyback{
		CONFIRM, targetAddr,
		MySwimInstance.ServerNewestPiggyback[targetAddr].ServerIncarnationNum,
	}
	MySwimInstance.PiggybackBuffer[piggyback] = 0
}

func deleteDeadServer(addr string) {
	deadServerIdx := -1
	for i, v := range config.MyConfig.PeersAddr {
		if v == addr {
			deadServerIdx = i
			break
		}
	}
	if deadServerIdx == -1 { // Already deleted
		return
	}
	config.MyConfig.PeersAddr = append(config.MyConfig.PeersAddr[0:deadServerIdx], config.MyConfig.PeersAddr[deadServerIdx+1:]...)
	delete(MySwimInstance.MembershipList, addr)
}

// IncrementPiggybackCount
// The passed piggybacks is the piggybacks from the caller, carried in PING & PINGREQ messages
// If the reply is valid and is ACK, then we know these piggybacks carried in the messages are sent successfully;
// then we increment the sending count in PiggybackBuffer
func (swim *SWIM) IncrementPiggybackCount(piggybacks []Piggyback) {
	swim.Mu.Lock()
	for _, piggyback := range piggybacks {
		swim.PiggybackBuffer[piggyback]++
	}
	swim.Mu.Unlock()
}

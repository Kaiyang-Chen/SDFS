package Sdfs

import(
	"encoding/json"
	"log"
	// "os"
	"sync"
	// "fmt"
	"sort"
	"strings"
	"CS425MP2/config"
)



func HandleSdfsMessage(request []byte) (string, []byte) {
	var message FileMessage
	err := json.Unmarshal(request, &message)
	if err != nil {
		log.Println("[Unmarshal]: ", err)
	}

	var reply FileMessage
	// var filePath string

	if message.MessageType  == FILESENT {
		// fmt.Printf("receive file sent message")
		return SdfsClient.HandleFileSent(message)
	}else if message.MessageType == TARGETREQ {
		reply, err = SdfsClient.HandleTargetReq(message);
	}else if message.MessageType == MASTERUPDATE{
		reply, err = SdfsClient.HandleMasterUpdate(message);
	}
	
	jsonReply, err := json.Marshal(reply)
	if err != nil {
		log.Println("[Marshal]: ", err)
	}
	return "", jsonReply
}

func (sdfs *SDFSClient) HandleFileSent(message FileMessage) (string, []byte){
	log.Printf("[HandleFileSent]: message=%v", message)
	sdfs.LocalMutex.Lock()
	defer sdfs.LocalMutex.Unlock()
	sdfs.LocalTable[message.FileName] = FileAddr{len(message.ReplicaAddr), message.ReplicaAddr}
	return PathPrefix + message.FileName, nil

}



func (sdfs *SDFSClient) allocateAddr() []string{
	var addrs []string
	if len(sdfs.ResourceDistribution) < COPYNUM {
		for k := range sdfs.ResourceDistribution {
			addrs = append(addrs, k)
		}
	} else {
		keys := make([]string, 0, len(sdfs.ResourceDistribution))

		for k := range sdfs.ResourceDistribution {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return sdfs.ResourceDistribution[keys[i]].NumReplica < sdfs.ResourceDistribution[keys[j]].NumReplica })
		addrs = keys[:COPYNUM]
	}
	return addrs
}

func (sdfs *SDFSClient)HandleTargetReq(message FileMessage) (FileMessage, error){
	log.Printf("[HandleTargetReq]: message=%v", message)
	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	var reply FileMessage
	_, exist := sdfs.MasterTable[message.FileName]
	if !exist {
		addrs := sdfs.allocateAddr()
		sdfs.MasterTable[message.FileName] = FileAddr{len(addrs), addrs}
		sdfs.ResourceMutex.Lock()
		defer sdfs.ResourceMutex.Unlock()
		for _, addr := range addrs {
			if entry, ok := sdfs.ResourceDistribution[addr]; ok {
				entry.NumReplica = entry.NumReplica + 1
				entry.StoreAddr = append(entry.StoreAddr, message.FileName)
				sdfs.ResourceDistribution[addr] = entry
			}
		}
		// hot update mastertable changes to its replica
		wg := sync.WaitGroup{}
		wg.Add(len(sdfs.ReplicaAddr.StoreAddr))
		for _, addr := range sdfs.ReplicaAddr.StoreAddr {
			go func(address string) {
				// key := message.FileName
				// value := sdfs.MasterTable[message.FileName]
				// tmp := map[string]FileAddr{key:value}
				sdfs.SendTableCopy(address, sdfs.MasterTable)
				wg.Done()
			}(addr)
		}
		wg.Wait()
		
	} 
	fileInfo, _ := sdfs.MasterTable[message.FileName]
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: TARGETSENT,
		TargetAddr:  "",
		FileName: 	 message.FileName,
		ReplicaAddr: fileInfo.StoreAddr,
		CopyTable:	nil,
	}
	return reply, nil

}

func (sdfs *SDFSClient)HandleMasterUpdate(message FileMessage) (FileMessage, error){
	log.Printf("[HandleMasterUpdate]: message=%v", message)
	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	// for f, addr := range message.CopyTable {
	// 	sdfs.MasterTable[f] = addr
	// }
	sdfs.MasterTable = message.CopyTable
	reply := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  message.SenderAddr,
		FileName: 	 "",
		ReplicaAddr: nil,
		CopyTable:	nil,
	}
	return reply, nil
}

func(sdfs *SDFSClient) HandleJoin(addr string){
	sdfs.ResourceMutex.Lock()
	defer sdfs.ResourceMutex.Unlock()
	sdfsAddr := strings.Split(addr, ":")[0] + ":" + "8889"
	sdfs.ResourceDistribution[sdfsAddr] = FileAddr{}
}


func (sdfs *SDFSClient)HandleLeaving(addr string){
	
	sdfs.ResourceMutex.Lock()
	sdfsAddr := strings.Split(addr, ":")[0] + ":" + "8889"
	delete(sdfs.ResourceDistribution, sdfsAddr)
	sdfs.ResourceMutex.Unlock()

	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	for k, v := range sdfs.MasterTable {
		idx := -1
		for i, tmpAdd := range v.StoreAddr {
			if tmpAdd == sdfsAddr {
				idx = i
			}
		}
		if idx >= 0{
			if entry, ok := sdfs.MasterTable[k]; ok {
				tmp := v.StoreAddr
				tmp[idx] = tmp[len(v.StoreAddr)-1]
				entry.StoreAddr = tmp[:len(v.StoreAddr)-1]
				entry.NumReplica = v.NumReplica-1
				sdfs.MasterTable[k] = entry
			}
		}
	}
}



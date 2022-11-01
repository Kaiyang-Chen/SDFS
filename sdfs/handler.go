package Sdfs

import(
	"encoding/json"
	"log"
	"os"
	"path"
	"sync"
	"fmt"
	"sort"
	"strings"
	"CS425MP2/config"
	"errors"
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
	}else if message.MessageType == SENTFILEREQ{
		reply, err = SdfsClient.HandleFileSentReq(message);
	}else if message.MessageType == GETFILEREQ{
		reply, err = SdfsClient.HandleGetFileReq(message);
	}else if message.MessageType == FILEDELETEREQ{
		reply, err = SdfsClient.HandleDeleteFileReq(message);
	}else if message.MessageType == FILEDELETE{
		reply, err = SdfsClient.HandleDeleteFile(message);
	}
	
	jsonReply, err := json.Marshal(reply)
	if err != nil {
		log.Println("[Marshal]: ", err)
	}
	return "", jsonReply
}

func (sdfs *SDFSClient) HandleFileSent(message FileMessage) (string, []byte){
	log.Printf("[HandleFileSent]: message=%v", message)
	fmt.Printf("[HandleFileSent]: message=%v", message)
	if message.ReplicaAddr == nil {
		var localFilePath string
		for key  := range message.CopyTable {
			localFilePath = key
		}
		dir := path.Dir(localFilePath)
		_, err := os.Stat(dir)
		if err != nil {
			os.Mkdir(dir, 0755)
		}
		return localFilePath, nil
	} else {
		sdfs.LocalMutex.Lock()
		defer sdfs.LocalMutex.Unlock()
		sdfs.LocalTable[message.FileName] = FileAddr{len(message.ReplicaAddr), message.ReplicaAddr}
		return PathPrefix + message.FileName, nil
	}
	

}



func (sdfs *SDFSClient) allocateAddr(num int) []string{
	var addrs []string
	if len(sdfs.ResourceDistribution) < num {
		for k := range sdfs.ResourceDistribution {
			addrs = append(addrs, k)
		}
	} else {
		keys := make([]string, 0, len(sdfs.ResourceDistribution))

		for k := range sdfs.ResourceDistribution {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return sdfs.ResourceDistribution[keys[i]].NumReplica < sdfs.ResourceDistribution[keys[j]].NumReplica })
		addrs = keys[:num]
	}
	return addrs
}


func(sdfs *SDFSClient) HandleDeleteFile(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleDeleteFile]: message=%v", message)
	fmt.Printf("[HandleDeleteFile]: message=%v", message)
	sdfs.LocalMutex.Lock()
	defer sdfs.LocalMutex.Unlock()
	delete(sdfs.LocalTable, message.FileName)
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  "",
		FileName: 	 message.FileName,
		ReplicaAddr: nil,
		CopyTable:	nil,
	}
	return reply, nil
}


func(sdfs *SDFSClient) HandleDeleteFileReq(message FileMessage) (FileMessage, error){
	log.Printf("[HandleGetFileReq]: message=%v", message)
	fmt.Printf("[HandleGetFileReq]: message=%v", message)
	success := make(chan bool, sdfs.MasterTable[message.FileName].NumReplica)
	for _, addr := range sdfs.MasterTable[message.FileName].StoreAddr {
		go sdfs.DeleteFile(message.FileName, addr, &success)
	}
	ok := true
	for i := 0; i < sdfs.MasterTable[message.FileName].NumReplica; i++ {
		result := <-success
		ok = ok && result
	}
	var err error
	if !ok {
		err = errors.New("something didn't work")
		log.Println("Failed Deleting file\n")
		fmt.Println("Failed Deleting file\n")
	} else {
		victim := sdfs.MasterTable[message.FileName].StoreAddr
		sdfs.MasterMutex.Lock()
		delete(sdfs.MasterTable, message.FileName)
		sdfs.MasterMutex.Unlock()
		sdfs.ResourceMutex.Lock()
		for _, addr := range victim {
			idx := -1
			for k, v := range sdfs.ResourceDistribution[addr].StoreAddr{
				if v == message.FileName {
					idx = k
				}
			}
			if idx >= 0 {
				if entry, ok := sdfs.ResourceDistribution[addr]; ok {
					tmp := entry.StoreAddr
					tmp[idx] = tmp[len(entry.StoreAddr)-1]
					entry.StoreAddr = tmp[:entry.NumReplica-1]
					entry.NumReplica = entry.NumReplica-1
					sdfs.ResourceDistribution[addr] = entry
				}
			}
		}
		sdfs.ResourceMutex.Unlock()
		log.Println("Succeed Deleting file\n")
		fmt.Println("Succeed Deleting file\n")
	}
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  "",
		FileName: 	 message.FileName,
		ReplicaAddr: nil,
		CopyTable:	nil,
	}
	return reply, err
}


func(sdfs *SDFSClient) HandleGetFileReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleGetFileReq]: message=%v", message)
	fmt.Printf("[HandleGetFileReq]: message=%v", message)
	var err error
	for _, addr := range sdfs.MasterTable[message.FileName].StoreAddr {
		err = sdfs.SendFileReq(addr, message.FileName, message.TargetAddr, message.ReplicaAddr, message.CopyTable)
		if err == nil {
			break
		}
	}
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: FILESENTACK,
		TargetAddr:  "",
		FileName: 	 message.FileName,
		ReplicaAddr: nil,
		CopyTable:	nil,
	}
	return reply, err
}


func (sdfs *SDFSClient) HandleFileSentReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleFileSentReq]: message=%v", message)
	fmt.Printf("[HandleFileSentReq]: message=%v", message)
	success := make(chan bool, 1)
	reply, err := sdfs.SendFile(message.TargetAddr, PathPrefix+message.FileName, message.FileName, &success, message.ReplicaAddr, message.CopyTable)
	ok := <- success
	if err != nil || !ok{
		log.Println(err)
	}
	return reply, err
}


func (sdfs *SDFSClient)HandleTargetReq(message FileMessage) (FileMessage, error){
	log.Printf("[HandleTargetReq]: message=%v", message)
	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	var reply FileMessage
	_, exist := sdfs.MasterTable[message.FileName]
	if !exist {
		addrs := sdfs.allocateAddr(COPYNUM)
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



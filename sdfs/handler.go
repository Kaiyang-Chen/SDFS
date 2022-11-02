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
	"strconv"
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
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleTargetReq(message);
	}else if message.MessageType == MASTERUPDATE{
		reply, err = SdfsClient.HandleMasterUpdate(message);
	}else if message.MessageType == SENTFILEREQ{
		reply, err = SdfsClient.HandleFileSentReq(message);
	}else if message.MessageType == GETFILEREQ{
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleGetFileReq(message);
	}else if message.MessageType == FILEDELETEREQ{
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleDeleteFileReq(message);
	}else if message.MessageType == FILEDELETE{
		reply, err = SdfsClient.HandleDeleteFile(message);
	}else if message.MessageType == LISTFILE{
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleListFile(message);
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
		sdfs.VersionMutex.Lock()
		defer sdfs.VersionMutex.Unlock()
		fileName := message.FileName + strconv.Itoa(message.ActionID)
		sdfs.VersionTable[message.FileName] = append(sdfs.VersionTable[message.FileName],FileInfo{fileName, message.ActionID})
		return PathPrefix + fileName, nil
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
	sdfs.VersionMutex.Lock()
	delete(sdfs.VersionTable, message.FileName)
	sdfs.VersionMutex.Unlock()
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  "",
		FileName: 	 message.FileName,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID:	message.ActionID,
	}
	return reply, nil
}


func(sdfs *SDFSClient) HandleListFile(message FileMessage) (FileMessage, error){
	log.Printf("[HandleListFile]: message=%v", message)
	fmt.Printf("[HandleListFile]: message=%v", message)
	var reply FileMessage
	if _, ok := sdfs.MasterTable[message.FileName]; !ok {
		reply = FileMessage{
			SenderAddr:  config.MyConfig.GetSdfsAddr(),
			MessageType: ACKOWLEDGE,
			TargetAddr:  "",
			FileName: 	 "",
			ReplicaAddr: nil,
			CopyTable:	nil,
			ActionID:	sdfs.MasterIncarnationID,
		}
		
	} else {
		reply = FileMessage{
			SenderAddr:  config.MyConfig.GetSdfsAddr(),
			MessageType: ACKOWLEDGE,
			TargetAddr:  "",
			FileName: 	 message.FileName,
			ReplicaAddr: sdfs.MasterTable[message.FileName].StoreAddr,
			CopyTable:	nil,
			ActionID:	sdfs.MasterIncarnationID,
		}
	}
	return reply, nil
}


func(sdfs *SDFSClient) HandleDeleteFileReq(message FileMessage) (FileMessage, error){
	log.Printf("[HandleGetFileReq]: message=%v", message)
	fmt.Printf("[HandleGetFileReq]: message=%v", message)
	var reply FileMessage
	if _, ok := sdfs.MasterTable[message.FileName]; !ok {
		reply = FileMessage{
			SenderAddr:  config.MyConfig.GetSdfsAddr(),
			MessageType: ACKOWLEDGE,
			TargetAddr:  "",
			FileName: 	 "",
			ReplicaAddr: nil,
			CopyTable:	nil,
			ActionID:	sdfs.MasterIncarnationID,
		}
		return reply, nil
	}
	success := make(chan bool, sdfs.MasterTable[message.FileName].NumReplica)
	for _, addr := range sdfs.MasterTable[message.FileName].StoreAddr {
		go sdfs.DeleteFile(message.FileName, addr, &success, sdfs.MasterIncarnationID)
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
		for _, addr := range sdfs.ReplicaAddr.StoreAddr{
			copyTable := sdfs.MasterTable
			sdfs.SendTableCopy(addr, copyTable)
		}
		log.Println("Succeed Deleting file\n")
		fmt.Println("Succeed Deleting file\n")
	}
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  "",
		FileName: 	 message.FileName,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID: 	sdfs.MasterIncarnationID,
	}
	return reply, err
}


func(sdfs *SDFSClient) HandleGetFileReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleGetFileReq]: message=%v", message)
	fmt.Printf("[HandleGetFileReq]: message=%v", message)
	var err error
	// Future work: compare and return the newest version of file
	for _, addr := range sdfs.MasterTable[message.FileName].StoreAddr {
		err = sdfs.SendFileReq(addr, message.FileName, message.TargetAddr, message.ReplicaAddr, message.CopyTable, sdfs.MasterIncarnationID)
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
		ActionID: 	sdfs.MasterIncarnationID,
	}
	return reply, err
}


func (sdfs *SDFSClient) HandleFileSentReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleFileSentReq]: message=%v", message)
	fmt.Printf("[HandleFileSentReq]: message=%v", message)
	success := make(chan bool, 1)
	var filename string
	id := -1;
	for _,v := range sdfs.VersionTable[message.FileName]{
		if id == -1 || id < v.IncarnationID{
			id = v.IncarnationID
			filename = v.FileName
		}
	}
	reply, err := sdfs.SendFile(message.TargetAddr, PathPrefix+filename, message.FileName, &success, message.ReplicaAddr, message.CopyTable, message.ActionID)
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
		ActionID: 	sdfs.MasterIncarnationID,
	}
	return reply, nil

}

func (sdfs *SDFSClient)HandleMasterUpdate(message FileMessage) (FileMessage, error){
	log.Printf("[HandleMasterUpdate]: message=%v", message)
	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	sdfs.IDMutex.Lock()
	defer sdfs.IDMutex.Unlock()
	sdfs.MasterTable = message.CopyTable
	sdfs.MasterIncarnationID = message.ActionID
	reply := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  message.SenderAddr,
		FileName: 	 "",
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID:	0,
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



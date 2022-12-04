package Sdfs

import (
	"CS425MP2/config"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	// "sync"
)

func HandleSdfsMessage(request []byte) (bool, string, []byte) {
	var message FileMessage
	err := json.Unmarshal(request, &message)
	if err != nil {
		log.Println("[Unmarshal]: ", err)
	}

	var reply FileMessage
	// var filePath string

	if message.MessageType == FILESENT {
		// fmt.Printf("receive file sent message")
		return SdfsClient.HandleFileSent(message)
	} else if message.MessageType == TARGETREQ {
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleTargetReq(message)
	} else if message.MessageType == MASTERUPDATE {
		reply, err = SdfsClient.HandleMasterUpdate(message)
	} else if message.MessageType == SENTFILEREQ {
		reply, err = SdfsClient.HandleFileSentReq(message)
	} else if message.MessageType == GETFILEREQ {
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleGetFileReq(message)
	} else if message.MessageType == FILEDELETEREQ {
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleDeleteFileReq(message)
	} else if message.MessageType == FILEDELETE {
		reply, err = SdfsClient.HandleDeleteFile(message)
	} else if message.MessageType == LISTFILE {
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleListFile(message)
	} else if message.MessageType == GETVFILEREQ {
		SdfsClient.IncreaseIncarnationID()
		reply, err = SdfsClient.HandleGetVersionsFileReq(message)
	} else if message.MessageType == SETLEADER {
		reply, err = SdfsClient.HandleSetLeader(message)
	}

	jsonReply, err := json.Marshal(reply)
	if err != nil {
		log.Println("[Marshal]: ", err)
	}
	return false, "", jsonReply
}

func (sdfs *SDFSClient) HandleFileSent(message FileMessage) (bool, string, []byte) {
	log.Printf("[HandleFileSent]: message=%v", message)
	// fmt.Printf("[HandleFileSent]: message=%v", message)
	if message.ReplicaAddr == nil {
		var localFilePath string
		for key := range message.CopyTable {
			localFilePath = key
		}
		dir := path.Dir(localFilePath)
		_, err := os.Stat(dir)
		if err != nil {
			os.Mkdir(dir, 0755)
		}
		if message.NumVersion == 1 {
			return true, localFilePath, nil
		}
		return false, localFilePath, nil
	} else {
		sdfs.LocalMutex.Lock()
		defer sdfs.LocalMutex.Unlock()
		sdfs.LocalTable[message.FileName] = FileAddr{len(message.ReplicaAddr), message.ReplicaAddr}
		sdfs.VersionMutex.Lock()
		defer sdfs.VersionMutex.Unlock()
		fileName := message.FileName + strconv.Itoa(message.ActionID)
		sdfs.VersionTable[message.FileName] = append(sdfs.VersionTable[message.FileName], FileInfo{fileName, message.ActionID})
		return false, PathPrefix + fileName, nil
	}

}

func (sdfs *SDFSClient) allocateAddr(num int) []string {
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
		sort.Slice(keys, func(i, j int) bool {
			return sdfs.ResourceDistribution[keys[i]].NumReplica < sdfs.ResourceDistribution[keys[j]].NumReplica
		})
		addrs = keys[:num]
	}
	return addrs
}

func (sdfs *SDFSClient) HandleSetLeader(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleSetLeader]: message=%v", message)
	// fmt.Printf("[HandleSetLeader]: message=%v", message)
	config.MyConfig.ChangeLeader(message.FileName)
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:    config.MyConfig.GetSdfsAddr(),
		MessageType:   ACKOWLEDGE,
		TargetAddr:    "",
		FileName:      "",
		ReplicaAddr:   nil,
		CopyTable:     nil,
		ActionID:      0,
		NumVersion:    0,
		ResourceTable: nil,
	}
	return reply, nil
}

func (sdfs *SDFSClient) HandleDeleteFile(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleDeleteFile]: message=%v", message)
	// fmt.Printf("[HandleDeleteFile]: message=%v", message)
	sdfs.LocalMutex.Lock()
	defer sdfs.LocalMutex.Unlock()
	delete(sdfs.LocalTable, message.FileName)
	sdfs.VersionMutex.Lock()
	delete(sdfs.VersionTable, message.FileName)
	sdfs.VersionMutex.Unlock()
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:    config.MyConfig.GetSdfsAddr(),
		MessageType:   ACKOWLEDGE,
		TargetAddr:    "",
		FileName:      message.FileName,
		ReplicaAddr:   nil,
		CopyTable:     nil,
		ActionID:      message.ActionID,
		NumVersion:    0,
		ResourceTable: nil,
	}
	return reply, nil
}

func (sdfs *SDFSClient) HandleListFile(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleListFile]: message=%v", message)
	// fmt.Printf("[HandleListFile]: message=%v", message)
	var reply FileMessage
	if _, ok := sdfs.MasterTable[message.FileName]; !ok {
		reply = FileMessage{
			SenderAddr:    config.MyConfig.GetSdfsAddr(),
			MessageType:   ACKOWLEDGE,
			TargetAddr:    "",
			FileName:      "",
			ReplicaAddr:   nil,
			CopyTable:     nil,
			ActionID:      sdfs.MasterIncarnationID,
			NumVersion:    0,
			ResourceTable: nil,
		}

	} else {
		reply = FileMessage{
			SenderAddr:    config.MyConfig.GetSdfsAddr(),
			MessageType:   ACKOWLEDGE,
			TargetAddr:    "",
			FileName:      message.FileName,
			ReplicaAddr:   sdfs.MasterTable[message.FileName].StoreAddr,
			CopyTable:     nil,
			ActionID:      sdfs.MasterIncarnationID,
			NumVersion:    0,
			ResourceTable: nil,
		}
	}
	return reply, nil
}

func (sdfs *SDFSClient) HandleDeleteFileReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleGetFileReq]: message=%v", message)
	// fmt.Printf("[HandleGetFileReq]: message=%v", message)
	var reply FileMessage
	if _, ok := sdfs.MasterTable[message.FileName]; !ok {
		reply = FileMessage{
			SenderAddr:    config.MyConfig.GetSdfsAddr(),
			MessageType:   ACKOWLEDGE,
			TargetAddr:    "",
			FileName:      "",
			ReplicaAddr:   nil,
			CopyTable:     nil,
			ActionID:      sdfs.MasterIncarnationID,
			NumVersion:    0,
			ResourceTable: nil,
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
		log.Println("Failed Deleting file")
		fmt.Println("Failed Deleting file")
	} else {
		victim := sdfs.MasterTable[message.FileName].StoreAddr
		sdfs.MasterMutex.Lock()
		delete(sdfs.MasterTable, message.FileName)
		sdfs.MasterMutex.Unlock()
		sdfs.ResourceMutex.Lock()
		for _, addr := range victim {
			idx := -1
			for k, v := range sdfs.ResourceDistribution[addr].StoreAddr {
				if v == message.FileName {
					idx = k
				}
			}
			if idx >= 0 {
				if entry, ok := sdfs.ResourceDistribution[addr]; ok {
					tmp := entry.StoreAddr
					for i := idx; i < len(entry.StoreAddr)-1; i++ {
						tmp[i] = tmp[i+1]
					}
					// tmp[idx] = tmp[len(entry.StoreAddr)-1]
					entry.StoreAddr = tmp[:entry.NumReplica-1]
					entry.NumReplica = entry.NumReplica - 1
					sdfs.ResourceDistribution[addr] = entry
				}
			}
		}
		sdfs.ResourceMutex.Unlock()
		for _, addr := range sdfs.ReplicaAddr.StoreAddr {
			copyTable := sdfs.MasterTable
			sdfs.SendTableCopy(addr, copyTable)
		}
		log.Println("Succeed Deleting file\n")
		fmt.Println("Succeed Deleting file\n")
	}
	reply = FileMessage{
		SenderAddr:    config.MyConfig.GetSdfsAddr(),
		MessageType:   ACKOWLEDGE,
		TargetAddr:    "",
		FileName:      message.FileName,
		ReplicaAddr:   nil,
		CopyTable:     nil,
		ActionID:      sdfs.MasterIncarnationID,
		NumVersion:    0,
		ResourceTable: nil,
	}
	return reply, err
}

func (sdfs *SDFSClient) HandleGetFileReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleGetFileReq]: message=%v", message)
	// fmt.Printf("[HandleGetFileReq]: message=%v", message)
	var err error
	// Future work: compare and return the newest version of file
	for _, addr := range sdfs.MasterTable[message.FileName].StoreAddr {
		err = sdfs.SendFileReq(addr, message.FileName, message.TargetAddr, message.ReplicaAddr, message.CopyTable, sdfs.MasterIncarnationID, 1)
		if err == nil {
			break
		}
	}
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:    config.MyConfig.GetSdfsAddr(),
		MessageType:   FILESENTACK,
		TargetAddr:    "",
		FileName:      message.FileName,
		ReplicaAddr:   nil,
		CopyTable:     nil,
		ActionID:      sdfs.MasterIncarnationID,
		NumVersion:    0,
		ResourceTable: nil,
	}
	return reply, err
}

func (sdfs *SDFSClient) HandleGetVersionsFileReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleGetFileReq]: message=%v", message)
	// fmt.Printf("[HandleGetFileReq]: message=%v", message)
	var err error
	for _, addr := range sdfs.MasterTable[message.FileName].StoreAddr {
		err = sdfs.SendFileReq(addr, message.FileName, message.TargetAddr, message.ReplicaAddr, message.CopyTable, sdfs.MasterIncarnationID, message.NumVersion)
		if err == nil {
			break
		}
	}
	var reply FileMessage
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: FILESENTACK,
		TargetAddr:  "",
		FileName:    message.FileName,
		ReplicaAddr: nil,
		CopyTable:   nil,
		ActionID:    sdfs.MasterIncarnationID,
		NumVersion:  0,
	}
	return reply, err
}

func (sdfs *SDFSClient) HandleFileSentReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleFileSentReq]: message=%v", message)
	// fmt.Printf("[HandleFileSentReq]: message=%v", message)
	sort.Sort(FileVersion(sdfs.VersionTable[message.FileName]))
	var filenames []string
	numV := 1
	if message.NumVersion < len(sdfs.VersionTable[message.FileName]) {
		numV = message.NumVersion
	} else {
		numV = len(sdfs.VersionTable[message.FileName])
	}
	for i := 0; i < numV; i++ {
		filenames = append(filenames, sdfs.VersionTable[message.FileName][i].FileName)
	}
	var reply FileMessage
	var err error
	success := make(chan bool, numV)
	for _, filename := range filenames {
		reply, err = sdfs.SendFile(message.TargetAddr, PathPrefix+filename, message.FileName, &success, message.ReplicaAddr, message.CopyTable, message.ActionID, 1)
		ok := <-success
		if err != nil || !ok {
			log.Println(err)
		}
	}

	return reply, err
}

func (sdfs *SDFSClient) HandleTargetReq(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleTargetReq]: message=%v", message)
	// fmt.Printf("[HandleTargetReq]: message=%v", message)
	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	var reply FileMessage
	_, exist := sdfs.MasterTable[message.FileName]
	if !exist {
		addrs := sdfs.allocateAddr(COPYNUM)
		sdfs.MasterTable[message.FileName] = FileAddr{len(addrs), addrs}
		// fmt.Println(sdfs.MasterTable[message.FileName])
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
		// wg := sync.WaitGroup{}
		// wg.Add(len(sdfs.ReplicaAddr.StoreAddr))
		// for _, addr := range sdfs.ReplicaAddr.StoreAddr {
		// 	go func(address string) {
		// 		sdfs.SendTableCopy(address, sdfs.MasterTable)
		// 		wg.Done()
		// 		fmt.Println(address)
		// 	}(addr)
		// }
		// wg.Wait()

	}
	fileInfo, _ := sdfs.MasterTable[message.FileName]
	// fmt.Println(sdfs.MasterTable[message.FileName].StoreAddr)
	reply = FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: TARGETSENT,
		TargetAddr:  "",
		FileName:    message.FileName,
		ReplicaAddr: fileInfo.StoreAddr,
		CopyTable:   nil,
		ActionID:    sdfs.MasterIncarnationID,
		NumVersion:  0,
	}
	// fmt.Printf("[HandleTargetReqReply]: message=%v", reply)
	return reply, nil

}

func (sdfs *SDFSClient) HandleMasterUpdate(message FileMessage) (FileMessage, error) {
	log.Printf("[HandleMasterUpdate]: message=%v", message)
	sdfs.MasterMutex.Lock()
	defer sdfs.MasterMutex.Unlock()
	sdfs.IDMutex.Lock()
	defer sdfs.IDMutex.Unlock()
	sdfs.MasterTable = message.CopyTable
	sdfs.MasterIncarnationID = message.ActionID
	sdfs.ReplicaAddr.StoreAddr = message.ReplicaAddr
	sdfs.ReplicaAddr.NumReplica = len(message.ReplicaAddr)
	sdfs.ResourceDistribution = message.ResourceTable
	reply := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: ACKOWLEDGE,
		TargetAddr:  message.SenderAddr,
		FileName:    "",
		ReplicaAddr: nil,
		CopyTable:   nil,
		ActionID:    0,
		NumVersion:  0,
	}
	return reply, nil
}

func (sdfs *SDFSClient) HandleJoin(addr string) {
	sdfs.ResourceMutex.Lock()
	IDunnoMaster.ResourceMutex.Lock()
	defer IDunnoMaster.ResourceMutex.Unlock()
	defer sdfs.ResourceMutex.Unlock()
	sdfsAddr := strings.Split(addr, ":")[0] + ":" + "8889"
	sdfs.ResourceDistribution[sdfsAddr] = FileAddr{}
	idunnoAddr := strings.Split(addr, ":")[0] + ":" + "8890"
	IDunnoMaster.ResourceTable[idunnoAddr] = ""
}

func (sdfs *SDFSClient) IsNextLeader() bool {
	if len(sdfs.ReplicaAddr.StoreAddr) > 0 && sdfs.ReplicaAddr.StoreAddr[0] == config.MyConfig.GetSdfsAddr() {
		return true
	}
	return false
}

func (sdfs *SDFSClient) HandleNewMaster() {
	log.Printf("Becoming new master!\n")
	fmt.Printf("Becoming new master!\n")
	// leaderAddr := strings.Split(config.MyConfig.GetSdfsAddr(), ":")[0]
	config.MyConfig.ChangeLeader(config.MyConfig.GetSdfsAddr())
	sdfs.HandleLeaving(config.MyConfig.GetSdfsAddr())
	IDunnoMaster.HandleLeaving(config.MyConfig.GetIDunnoAddr())
	newReplicaAddr := sdfs.ReplicaAddr.StoreAddr[1:]
	sdfs.ReplicaAddr.StoreAddr = newReplicaAddr
	sdfs.ReplicaAddr.NumReplica = sdfs.ReplicaAddr.NumReplica - 1
	success := make(chan bool, len(sdfs.ResourceDistribution))
	for addr, _ := range sdfs.ResourceDistribution {
		// fmt.Printf("Updating master to %s for node  %s.\n", config.MyConfig.GetSdfsAddr(), addr)
		go sdfs.SendUpdatedMaster(config.MyConfig.GetSdfsAddr(), &success, addr)
	}
	ok := true
	for i := 0; i < len(sdfs.ResourceDistribution); i++ {
		result := <-success
		ok = ok && result
	}
	if !ok {
		log.Println("Failed Updating Master to all")
		fmt.Println("Failed Updating Master to all")
	}
	// go SdfsClient.PeriodicalCheckMaster()
	// go SdfsClient.PeriodicalCheckResource()

}

func (sdfs *SDFSClient) HandleLeaving(addr string) {

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
		if idx >= 0 {
			if entry, ok := sdfs.MasterTable[k]; ok {
				tmp := v.StoreAddr
				for i := idx; i < len(v.StoreAddr)-1; i++ {
					tmp[i] = tmp[i+1]
				}
				// tmp[idx] = tmp[len(v.StoreAddr)-1]
				entry.StoreAddr = tmp[:len(v.StoreAddr)-1]
				entry.NumReplica = v.NumReplica - 1
				sdfs.MasterTable[k] = entry
			}
		}
	}
}

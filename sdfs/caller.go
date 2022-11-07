package Sdfs
import (
	"CS425MP2/network"
	"CS425MP2/config"
	"encoding/json"
	"fmt"
	"log"
)

func (sdfs *SDFSClient) SendMessage(request FileMessage, host string, filePath string, sdfsName string) (FileMessage, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Println(err)
	}
	response, err := network.SdfsDial(host, filePath, sdfsName, jsonData)
	replyMessage := FileMessage{}
	if err == nil {
		json.Unmarshal(response, &replyMessage)
	}
	return replyMessage, err
}

func(sdfs *SDFSClient) SendFile(host string, filePath string, sdfsName string, success *chan bool, repAddr []string, copyTable map[string]FileAddr, IncarID int, Type int) (FileMessage, error){
	fmt.Printf("Sending file %s.\n", sdfsName)
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: FILESENT,
		TargetAddr:  host,
		FileName: 	 sdfsName,
		ReplicaAddr: repAddr,
		CopyTable:	copyTable,
		ActionID: 	IncarID,
		NumVersion:	Type,
		ResourceTable: nil,
	}
	reply, err := sdfs.SendMessage(message, host, filePath, sdfsName)
	if err != nil {
		*success <- false
	} else {
		*success <- true
	}
	return reply, err

}

func(sdfs *SDFSClient) SendFileReq(fileNode string, sdfsName string, targetAddr string, repAddr []string, copyTable map[string]FileAddr, IncarID int, numV int) error {
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: SENTFILEREQ,
		TargetAddr:  targetAddr,
		FileName: 	 sdfsName,
		ReplicaAddr: repAddr,
		CopyTable:	copyTable,
		ActionID:	IncarID,
		NumVersion:	numV,
		ResourceTable: nil,
	}
	_, err := sdfs.SendMessage(message, fileNode, "", "")
	return err
}


func(sdfs *SDFSClient) GetFile(filePath string, sdfsName string) error{
	LocalFilePath := make(map[string]FileAddr) // to store the local file path 
	LocalFilePath[filePath] = FileAddr{}
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: GETFILEREQ,
		TargetAddr:  config.MyConfig.GetSdfsAddr(),
		FileName: 	 sdfsName,
		ReplicaAddr: nil,
		CopyTable:	LocalFilePath,
		ActionID:	0,
		NumVersion:	0,
		ResourceTable: nil,
	}
	_, err := sdfs.SendMessage(message, config.MyConfig.GetLeaderAddr(), "", "")
	if err != nil {
		log.Println(err)
	}
	return err
}


func(sdfs *SDFSClient) GetVersionsFile(sdfsName string, numVersion int, filePath string) error{
	LocalFilePath := make(map[string]FileAddr) // to store the local file path 
	LocalFilePath[filePath] = FileAddr{}
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: GETVFILEREQ,
		TargetAddr:  config.MyConfig.GetSdfsAddr(),
		FileName: 	 sdfsName,
		ReplicaAddr: nil,
		CopyTable:	LocalFilePath,
		ActionID:	0,
		NumVersion:	numVersion,
		ResourceTable: nil,
	}
	_, err := sdfs.SendMessage(message, config.MyConfig.GetLeaderAddr(), "", "")
	if err != nil {
		log.Println(err)
	}
	return err
}


func(sdfs *SDFSClient) DeleteFile(sdfsName string, address string, success *chan bool, IncarID int) (FileMessage, error) {
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: FILEDELETE,
		TargetAddr:  address,
		FileName: 	 sdfsName,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID:	IncarID,
		NumVersion:	0,
		ResourceTable: nil,
	}
	reply, err := sdfs.SendMessage(message, address, "", "")
	if err != nil {
		*success <- false
	} else {
		*success <- true
	}
	return reply, err
}


func(sdfs *SDFSClient) ListFileReq(sdfsName string) error {
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: LISTFILE,
		TargetAddr:  config.MyConfig.GetLeaderAddr(),
		FileName: 	 sdfsName,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID:	0,
		NumVersion:	0,
		ResourceTable: nil,
	}
	reply, err := sdfs.SendMessage(message, config.MyConfig.GetLeaderAddr(), "", "")

	if err != nil {
		log.Println(err)
		return err
	}
	if reply.FileName == "" {
		fmt.Printf("No such file in sdfs!\n")
	} else {
		fmt.Printf("File %s stored at :", sdfsName)
		for _ , v := range reply.ReplicaAddr {
			fmt.Printf(" %s ", v)
		}
		fmt.Printf("\n")
	}
	return err
}


func(sdfs *SDFSClient) DeleteFileReq(sdfsName string) error {
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: FILEDELETEREQ,
		TargetAddr:  config.MyConfig.GetLeaderAddr(),
		FileName: 	 sdfsName,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID:	0,
		NumVersion:	0,
		ResourceTable: nil,
	}
	reply, err := sdfs.SendMessage(message, config.MyConfig.GetLeaderAddr(), "", "")
	if err != nil {
		log.Println(err)
	}
	if reply.FileName == "" {
		fmt.Printf("No such file in sdfs!\n")
	}
	return err
}


func(sdfs *SDFSClient) PutFile(filePath string, sdfsName string) error{
	log.Println("Put file!\n")
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: TARGETREQ,
		TargetAddr:  config.MyConfig.GetLeaderAddr(),
		FileName: 	 sdfsName,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID:	0,
		NumVersion:	0,
		ResourceTable: nil,
	}
	reply, err := sdfs.SendMessage(message, config.MyConfig.GetLeaderAddr(), "", sdfsName)
	fmt.Printf("[AckHandleTargetReq]: message=%v", reply)
	if err != nil {
		log.Println(err)
		return err
	}
	success := make(chan bool, len(reply.ReplicaAddr))
	for _, addr := range reply.ReplicaAddr {
		go sdfs.SendFile(addr, filePath, sdfsName, &success, reply.ReplicaAddr, nil, reply.ActionID, 0)
	}
	ok := false
	for i := 0; i < len(reply.ReplicaAddr); i++ {
		result := <-success
		ok = ok || result
	}
	if !ok {
		log.Println("Failed putting file\n")
		fmt.Println("Failed putting file\n")
	} else{
		log.Println("Succeed putting file\n")
		fmt.Println("Succeed putting file\n")
	}
	return nil

}


func(sdfs *SDFSClient) SendUpdatedMaster(leaderAddr string, success *chan bool, addr string) error {
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: SETLEADER,
		TargetAddr:  addr,
		FileName: 	 leaderAddr,
		ReplicaAddr: nil,
		CopyTable:	nil,
		ActionID: 	sdfs.MasterIncarnationID,
		NumVersion:	0,
		ResourceTable: nil,
	}
	_, err := sdfs.SendMessage(message, addr, "", "")
	if err != nil {
		*success <- false
	} else {
		*success <- true
	}
	return err
}


func (sdfs *SDFSClient) SendTableCopy(host string, table map[string]FileAddr)  {
	log.Printf("[SendTableCopy]\n")
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: MASTERUPDATE,
		TargetAddr:  host,
		FileName: 	 "",
		ReplicaAddr: sdfs.ReplicaAddr.StoreAddr,
		CopyTable:	table,
		ActionID: 	sdfs.MasterIncarnationID,
		NumVersion:	0,
		ResourceTable: sdfs.ResourceDistribution,
	}
	sdfs.SendMessage(message, host, "", "")

	return 
}

func (sdfs *SDFSClient) ShowLocalTable() {
	for k, _ := range sdfs.LocalTable {
		fmt.Printf("Filename: %s\n", k)
	}
}

func (sdfs *SDFSClient) ShowMasterTable() {
	for k, v := range sdfs.MasterTable {
		fmt.Printf("Filename: %s\n", k)
		fmt.Printf("Stored at : ")
		for _, add := range v.StoreAddr{
			fmt.Printf("%s ", add)
		}
		fmt.Printf("\n")
	}
}

func (sdfs *SDFSClient) ShowResourceDistribution() {
	for k, v := range sdfs.ResourceDistribution {
		fmt.Printf("Storage node: %s\n", k)
		fmt.Printf("Stored file : ")
		for _, add := range v.StoreAddr{
			fmt.Printf("%s ", add)
		}
		fmt.Printf("\n")
	}
}

func (sdfs *SDFSClient) ShowVersionTable() {
	for k, v := range sdfs.VersionTable {
		fmt.Printf("Filename: %s\n", k)
		fmt.Printf("Stored file : ")
		for _, i := range v{
			fmt.Printf("%s ", i.FileName)
		}
		fmt.Printf("\n")
	}
}



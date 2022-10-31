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

func(sdfs *SDFSClient) SendFile(host string, filePath string, sdfsName string, success *chan bool, repAddr []string) (FileMessage, error){
	fmt.Printf("Sending file %s.\n", sdfsName)
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: FILESENT,
		TargetAddr:  host,
		FileName: 	 sdfsName,
		ReplicaAddr: repAddr,
		CopyTable:	nil,
	}
	reply, err := sdfs.SendMessage(message, host, filePath, sdfsName)
	if err != nil {
		*success <- false
	} else {
		*success <- true
	}
	return reply, err

}

func(sdfs *SDFSClient) SendFileReq(fileNode string, sdfsName string, targetAddr string, repAddr []string) error {
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: SENTFILEREQ,
		TargetAddr:  targetAddr,
		FileName: 	 sdfsName,
		ReplicaAddr: repAddr,
		CopyTable:	nil,
	}
	_, err := sdfs.SendMessage(message, fileNode, "", "")
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
	}
	reply, err := sdfs.SendMessage(message, config.MyConfig.GetLeaderAddr(), "", sdfsName)

	if err != nil {
		log.Println(err)
		return err
	}
	success := make(chan bool, len(reply.ReplicaAddr))
	for _, addr := range reply.ReplicaAddr {
		go sdfs.SendFile(addr, filePath, sdfsName, &success, reply.ReplicaAddr)
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


func (sdfs *SDFSClient) SendTableCopy(host string, table map[string]FileAddr)  {
	log.Printf("[SendTableCopy]\n")
	message := FileMessage{
		SenderAddr:  config.MyConfig.GetSdfsAddr(),
		MessageType: MASTERUPDATE,
		TargetAddr:  host,
		FileName: 	 "",
		ReplicaAddr: nil,
		CopyTable:	table,
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



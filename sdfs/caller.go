package Sdfs
import (
	"CS425MP2/network"
	"CS425MP2/config"
	"encoding/json"
	"fmt"
	"log"
)

func (sdfs *SDFSClient) SendMessage(request FileMessgae, host string, filePath string) (string, error) {
	jsonData, err := json.Marshal(request)
	if err != nil {
		log.Println(err)
	}
	response, err := network.SdfsDial(host, filePath, jsonData)
	var replyMessage string
	if err == nil {
		err = json.Unmarshal(response, &replyMessage)
	}
	return replyMessage, err
}

func(sdfs *SDFSClient) SendFile(host string, filePath string){
	fmt.Printf("sending file \n")
	message := FileMessgae{
		SenderAddr:  config.MyConfig.GetMyAddr(),
		MessageType: FILESENT,
		TargetAddr:  host,
		FileName: 	 filePath,
	}
	response, _ := sdfs.SendMessage(message, host, filePath)
	fmt.Println(response)

}
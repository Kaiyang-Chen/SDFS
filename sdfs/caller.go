package Sdfs
import (
	"CS425MP2/network"
	"encoding/json"
)


func(sdfs *SDFSClient) SendFile(host string, filePath string){
	response, err := network.SdfsDial(host, filePath)
	var replyMessage string
	if err == nil {
		err = json.Unmarshal(response, &replyMessage)
	}
}
package Sdfs

import(
	"encoding/json"
	"log"
)

const pathPrefix = "/home/kc68/files/"

func HandleMessage(request []byte) (string, []byte) {
	var message FileMessgae
	err := json.Unmarshal(request, &message)
	if err != nil {
		log.Println("[Unmarshal]: ", err)
	}

	// var reply FileMessgae
	// var filePath string

	if message.MessageType  == FILESENT {
		// TODO: change local file entry table
		return pathPrefix + message.FileName, nil
	}
	return "", nil

}
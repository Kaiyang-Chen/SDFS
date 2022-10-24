package Sdfs

import(
	"encoding/json"
	"log"
	"os"
	"fmt"
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
		fmt.Printf("receive file sent message")
		// TODO: change local file entry table
		fileInfo, _ := os.Stat(message.FileName)

		return pathPrefix + fileInfo.Name(), nil
	}
	return "", nil

}
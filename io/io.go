package io
import (
	"strings"
	"bufio"
	"os"
	"fmt"
	"CS425MP2/config"
	"CS425MP2/SWIM"
	"CS425MP2/sdfs"
)


func Handle_IO() {
	inputReader := bufio.NewReader(os.Stdin)
	for {
		input, err := inputReader.ReadString('\n')
		input = strings.Replace(input, "\n", "", -1)
		if err != nil || input == "" {
			fmt.Printf("Failed to read the input! Try again!\n")
			continue
		}

		if strings.Compare("leave", input) == 0 {
			if !config.MyConfig.IsIntroducer() {
				os.Exit(0)
			}
		}
		if strings.Compare("member", input) == 0 {
			SWIM.MySwimInstance.SwimShowPeer()
		}
		if strings.Contains(input, "put") {
			tmp := strings.Split(input, " ")
			Sdfs.SdfsClient.SendFile("fa22-cs425-4802.cs.illinois.edu:8889", tmp[1])
		}

	}
}
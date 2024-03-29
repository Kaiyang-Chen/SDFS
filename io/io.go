package io
import (
	"strings"
	"bufio"
	"os"
	"fmt"
	"CS425MP2/config"
	// "CS425MP2/SWIM"
	"CS425MP2/sdfs"
	"strconv"
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
			Sdfs.MySwimInstance.SwimShowPeer()
		}
		if strings.Compare("leader", input) == 0 {
			fmt.Println(config.MyConfig.GetLeaderAddr())
		}
		if strings.Compare("master", input) == 0 {
			Sdfs.SdfsClient.ShowMasterTable()
		}
		if strings.Compare("store", input) == 0 {
			Sdfs.SdfsClient.ShowLocalTable()
		}
		if strings.Compare("resource", input) == 0 {
			Sdfs.SdfsClient.ShowResourceDistribution()
		}
		if strings.Compare("version", input) == 0 {
			Sdfs.SdfsClient.ShowVersionTable()
		}
		if strings.Contains(input, "get"){
			tmp := strings.Split(input, " ")
			Sdfs.SdfsClient.GetFile(tmp[2], tmp[1])
		}
		if strings.Contains(input, "delete"){
			tmp := strings.Split(input, " ")
			Sdfs.SdfsClient.DeleteFileReq(tmp[1])
		}
		if strings.Contains(input, "ls"){
			tmp := strings.Split(input, " ")
			Sdfs.SdfsClient.ListFileReq(tmp[1])
		}
		if strings.Contains(input, "put") {
			fmt.Printf("putting \n")
			tmp := strings.Split(input, " ")
			Sdfs.SdfsClient.PutFile(tmp[1], tmp[2])
		}
		if strings.Compare("waitq", input) == 0 {
			Sdfs.IDunnoMaster.ShowWait(true)
		}
		if strings.Compare("runq", input) == 0 {
			Sdfs.IDunnoMaster.ShowRun(true)
		}
		if strings.Compare("C1", input) == 0 {
			Sdfs.IDunnoMaster.C1()
		}
		if strings.Compare("C2", input) == 0 {
			Sdfs.IDunnoMaster.C2()
		}
		if strings.Contains(input, "C3") {
			tmp := strings.Split(input, " ")
			batchSize, _ := strconv.Atoi(tmp[2])
			Sdfs.IDunnoMaster.C3(tmp[1], batchSize)
		}
		if strings.Contains(input, "C4") {
			tmp := strings.Split(input, " ")
			Sdfs.IDunnoMaster.C4(tmp[1])
		}
		if strings.Compare("C5", input) == 0 {
			Sdfs.IDunnoMaster.ShowResourceTable()
		}
		if strings.Contains(input, "get-versions") {
			fmt.Printf("putting \n")
			tmp := strings.Split(input, " ")
			numVersion, _ := strconv.Atoi(tmp[2])
			Sdfs.SdfsClient.GetVersionsFile(tmp[1], numVersion, tmp[3])
		}

	}
}
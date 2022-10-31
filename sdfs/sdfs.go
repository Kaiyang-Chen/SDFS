package Sdfs

import (
	"CS425MP2/config"
	// "CS425MP2/SWIM"
	"CS425MP2/network"
	"sync"
	"log"
	"fmt"
	"time"
	"os"
	"strings"
)

const MASTERCOPYNUM = 3
const COPYNUM = 4
const PathPrefix = "/home/kc68/files/"
// Type of message in sdfs
const (
	TARGETREQ = 0
	TARGETSENT = 1
	FILESENT = 2
	FILESENTACK = 3
	MASTERUPDATE = 4
	ACKOWLEDGE = 5
)

type FileAddr struct {
	NumReplica	int
	StoreAddr	[]string
}

type FileMessage struct {
	SenderAddr	string
	MessageType	int
	TargetAddr	string
	// TODO File sender datatype
	FileName	string
	ReplicaAddr []string
	CopyTable	map[string]FileAddr
}



type SDFSClient struct {
	MasterTable	map[string]FileAddr
	LocalTable	map[string]FileAddr
	ResourceDistribution	map[string]FileAddr
	ReplicaAddr	FileAddr
	MasterMutex  sync.RWMutex
	LocalMutex	sync.RWMutex
	ResourceMutex	sync.RWMutex
}

var SdfsClient SDFSClient

func InitSDFS() {
	fmt.Printf("init sdfs\n")
	SdfsClient.MasterTable = make(map[string]FileAddr)
	SdfsClient.LocalTable = make(map[string]FileAddr)
	SdfsClient.ReplicaAddr.NumReplica = 0
	SdfsClient.ResourceDistribution = make(map[string]FileAddr)
	if config.MyConfig.IsIntroducer() {
		go SdfsClient.PeriodicalCheck()
		
	}
	_, err := os.Stat(PathPrefix)
    if err != nil {
        os.Mkdir(PathPrefix, 0755)
    }
	go func() {
		fmt.Println(config.MyConfig.GetSdfsAddr())
		err := network.Listen(config.MyConfig.GetSdfsAddr(), HandleSdfsMessage)
		if err != nil {
			log.Fatal(err)
		}
	}()

}

func min(x int, y int) int{
	if x <= y {
		return x
	} else {
		return y
	}
}

func contains(s []string, e string) bool {
    for _, a := range s {
        if a == e {
            return true
        }
    }
    return false
}





// PeriodicalCheck
// Only called by Master in SDFS, to check whether the replica node for global table still alive. If not, send copy to new replica node.
func (sdfs *SDFSClient) PeriodicalCheck() {
	for{
		TmpMemList := MySwimInstance.SwimGetPeer()
		var NewCopyList	[]string
		for _, addr := range sdfs.ReplicaAddr.StoreAddr {
			if contains(TmpMemList, addr){
				sdfs.ReplicaAddr.NumReplica -= 1
			} else {
				NewCopyList = append(NewCopyList, addr)
			}
		}
		
		target := min(len(TmpMemList), MASTERCOPYNUM)
		if len(NewCopyList) < target {
			for _, addr := range TmpMemList {
				sdfsAddr := strings.Split(addr, ":")[0] + ":" + "8889"
				if !contains(sdfs.ReplicaAddr.StoreAddr, sdfsAddr){
					copyTable := sdfs.MasterTable
					sdfs.SendTableCopy(sdfsAddr, copyTable)
					sdfs.ReplicaAddr.NumReplica += 1
					NewCopyList = append(NewCopyList, sdfsAddr)
				}
				if sdfs.ReplicaAddr.NumReplica == target {
					break
				}
			}
		}
		sdfs.ReplicaAddr.StoreAddr = NewCopyList
		time.Sleep(2 * time.Second)
	}

}
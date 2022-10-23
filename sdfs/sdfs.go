package Sdfs

import (
	"CS425MP2/config"
	// "CS425MP2/SWIM"
	"CS425MP2/network"
	"sync"
	"log"
	// "golang.org/x/exp/slices"
)

const MASTERCOPYNUM = 3

// Type of message in sdfs
const (
	TARGETREQ = 0
	TARGETSENT = 1
	FILESENT = 2
	FILESENTACK = 3
)

type FileAddr struct {
	NumReplica	int
	StoreAddr	[]string
}

type FileMessgae struct {
	SenderAddr	string
	MessageType	int
	TargetAddr	string
	// TODO File sender datatype
	FileName	string

}


type SDFSClient struct {
	MasterTable	map[string]FileAddr
	LocalTable	map[string]FileAddr
	ResourceDistribution	map[string]int
	ReplicaAddr	FileAddr
	MasterMuex  sync.RWMutex
	LocalMutex	sync.RWMutex
}

var SdfsClient SDFSClient

func InitSDFS() {
	SdfsClient.MasterTable = make(map[string]FileAddr)
	SdfsClient.LocalTable = make(map[string]FileAddr)
	SdfsClient.ReplicaAddr.NumReplica = 0
	// if config.MyConfig.IsIntroducer() {
	// 	go PeriodicalCheck()
	// 	SdfsClient.ResourceDistribution = make(map[string]int)
	// }
	go func() {
		err := network.Listen(config.MyConfig.GetSdfsAddr(), HandleMessage)
		if err != nil {
			log.Fatal(err)
		}
	}()




}


// PeriodicalCheck
// Only called by Master in SDFS, to check whether the replica node for globle table still alive. If not, send copy to new replica node.
// func PeriodicalCheck() {
// 	TmpMemList = SWIM.MySwimInstance.SwimGetPeer()
// 	var NewCopyList	[]string
// 	for addr := range SdfsClient.ReplicaAddr.StoreAddr {
// 		if !slices.Contains(TmpMemList, addr){
// 			SdfsClient.ReplicaAddr.NumReplica -= 1
// 		} else {
// 			NewCopyList.append(addr)
// 		}
// 	}
	
// 	target := min(len(TmpMemList), MASTERCOPYNUM)
// 	if len(NewCopyList) < target {
// 		for addr := range TmpMemList {
// 			if !slices.Contains(SdfsClient.ReplicaAddr.StoreAddr, addr){
// 				//TODO: send copy to such addr
				
// 				SdfsClient.ReplicaAddr.NumReplica += 1
// 				NewCopyList.append(addr)
// 			}
// 			if SdfsClient.ReplicaAddr.NumReplica == target {
// 				break
// 			}
// 		}
// 	}
// 	SdfsClient.ReplicaAddr.StoreAddr = NewCopyList

// }
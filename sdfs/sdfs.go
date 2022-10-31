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
	SENTFILEREQ = 6
	GETFILEREQ = 7
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
		go SdfsClient.PeriodicalCheckMaster()
		go SdfsClient.PeriodicalCheckResource()
		// TODO: garbage collection

	}
	_, err := os.Stat(PathPrefix)
    if err != nil {
        os.Mkdir(PathPrefix, 0755)
    }
	go func() {
		// fmt.Println(config.MyConfig.GetSdfsAddr())
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


// PeriodicalCheckResource
// Only called by Master in SDFS, to check whether the replica number for file is sufficient. If not, send file copy to new replica node.
func (sdfs *SDFSClient) PeriodicalCheckResource() {
	for {
		TmpMemList := sdfs.ResourceDistribution
		target := min(len(TmpMemList), COPYNUM)
		for k, v := range sdfs.MasterTable{
			if v.NumReplica < target {
				potentialAddr := sdfs.allocateAddr(target)
				var newAddrs []string
				for _, tmpAddr := range potentialAddr{
					if !contains(v.StoreAddr, tmpAddr) {
						newAddrs = append(newAddrs, tmpAddr)
					}
					if v.NumReplica + len(newAddrs) == target {
						break
					}
				}
				fileNodes := v.StoreAddr
				sdfs.MasterMutex.Lock()
				if entry, ok := sdfs.MasterTable[k]; ok {
					entry.NumReplica = target
					for _, tmp := range newAddrs {
						entry.StoreAddr = append(entry.StoreAddr, tmp)
					}
					sdfs.MasterTable[k] = entry
				}
				sdfs.MasterMutex.Unlock()
				for _ , addr:= range newAddrs {
					for _, fileAddr := range fileNodes{
						fmt.Printf("call %s to sent file %s to %s.\n", fileAddr, k, addr)
						err := sdfs.SendFileReq(fileAddr, k, addr, sdfs.MasterTable[k].StoreAddr, nil)
						if err == nil {
							sdfs.ResourceMutex.Lock()
							if entry, ok := sdfs.ResourceDistribution[addr]; ok {
								entry.NumReplica = entry.NumReplica + 1
								entry.StoreAddr = append(entry.StoreAddr, k)
								sdfs.ResourceDistribution[addr] = entry
							}
							sdfs.ResourceMutex.Unlock()
							log.Printf("Peiroodical check: Send file Copy %s to node %s.\n",k,addr)
							fmt.Printf("Peiroodical check: Send file Copy %s to node %s.\n",k,addr)
							break
						}
					}
				}
			}
		}

		time.Sleep(5 * time.Second)
	}
}


// PeriodicalCheckMaster
// Only called by Master in SDFS, to check whether the replica node for global table still alive. If not, send copy to new replica node.
func (sdfs *SDFSClient) PeriodicalCheckMaster() {
	for{
		TmpMemList := MySwimInstance.SwimGetPeer()
		var NewCopyList	[]string
		for _, addr := range sdfs.ReplicaAddr.StoreAddr {
			sdfsAddr := strings.Split(addr, ":")[0] + ":" + "8888"
			if !contains(TmpMemList, sdfsAddr){
				sdfs.ReplicaAddr.NumReplica -= 1
			} else {
				NewCopyList = append(NewCopyList, addr)
			}
		}
		
		target := min(len(TmpMemList), MASTERCOPYNUM)
		if len(NewCopyList) < target {
			for _, addr := range TmpMemList {
				sdfsAddr := strings.Split(addr, ":")[0] + ":" + "8889"
				if !contains(NewCopyList, sdfsAddr){
					
					sdfs.ReplicaAddr.NumReplica += 1
					NewCopyList = append(NewCopyList, sdfsAddr)
				}
				if sdfs.ReplicaAddr.NumReplica == target {
					break
				}
			}
		}
		sdfs.ReplicaAddr.StoreAddr = NewCopyList
		for _, addr := range sdfs.ReplicaAddr.StoreAddr{
			copyTable := sdfs.MasterTable
			sdfs.SendTableCopy(addr, copyTable)
		}
		time.Sleep(1 * time.Second)
	}

}
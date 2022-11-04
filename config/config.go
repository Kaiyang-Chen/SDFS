package config

import (
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

const PORT = "8888"
const SDFSPORT = "8889"

type Config struct {
	ServerID       int      // A Unique identifier assigned by the introducer, each associated with server's address
	IP             string   // My IP address for the current process
	Port           string   // My port for the current process
	SdfsPort       string   // My port for the sdfs
	PeersAddr      []string // Address of my peers, with IP and Port
	IntroducerAddr string   // Address of the introducer in the group, which is pre-determined
	Mu             sync.RWMutex
}

// DEBUG
// If DEBUG is true, then we will test our program using multiple process in a single local machine;
// Otherwise we will run our program on VMs.
const DEBUG = false

var MyConfig = Config{-1, "", PORT, SDFSPORT, make([]string, 0), "", sync.RWMutex{}}

// InitConfig
// If DEBUG is true, then all process will use localhost with different ports;
// Otherwise we will use our VMs' address with port 8888
func InitConfig() {
	if DEBUG {
		MyConfig.IntroducerAddr = "127.0.0.1:8880"
	} else {
		MyConfig.IntroducerAddr = "fa22-cs425-4801.cs.illinois.edu:8888"
	}
	cmd := exec.Command("/usr/bin/hostname")
	res, _ := cmd.CombinedOutput()
	MyConfig.IP = string(strings.Replace(string(res), "\n", "", -1))
	clientIdStr := strings.Split(strings.Split(string(res), ".")[0], "-")[2][2:]
	clientId, _ := strconv.Atoi(clientIdStr)
	MyConfig.ServerID = clientId
	MyConfig.PeersAddr = append(MyConfig.PeersAddr, MyConfig.GetMyAddr())
	// Initially, a non-introducer node only knows the address of the introducer,
	// and the introducer will update my config via PING message, so I can know other peers
	if !MyConfig.IsIntroducer() {
		MyConfig.PeersAddr = append(MyConfig.PeersAddr, MyConfig.IntroducerAddr)
	}
}

func (config *Config) ChangeLeader(addr string) {
	config.Mu.Lock()
	config.IntroducerAddr = addr
	config.Mu.Unlock()
}

func (config *Config) GetMyAddr() string {
	return MyConfig.IP + ":" + MyConfig.Port
}

func (confif *Config) GetLeaderAddr() string {
	return strings.Split(MyConfig.IntroducerAddr, ":")[0] + ":" + MyConfig.SdfsPort
}

func (config *Config) GetSdfsAddr() string {
	return MyConfig.IP + ":" + MyConfig.SdfsPort
}

func (config *Config) IsIntroducer() bool {
	return config.GetMyAddr() == MyConfig.IntroducerAddr || config.GetSdfsAddr() == MyConfig.IntroducerAddr
}

func (config *Config) GetServerId() int {
	return MyConfig.ServerID
}

func (config *Config) GetPeersAddr() []string {
	return config.PeersAddr
}

func (config *Config) UpdateConfig(newConfig *Config) {
	config.Mu.Lock()
	config.PeersAddr = newConfig.PeersAddr
	config.Mu.Unlock()
}

func (config *Config) DeletePeer(address string) {

	for i, value := range config.PeersAddr {
		if value == address {
			config.Mu.Lock()
			config.PeersAddr = append(config.PeersAddr[:i], config.PeersAddr[i+1:]...)
			config.Mu.Unlock()
			break
		}
	}
	return
}

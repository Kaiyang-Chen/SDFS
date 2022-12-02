package Sdfs

import (
	"fmt"
	"net"
	"net/rpc"
	"os"
	"sync"
	"strings"
	"time"
	"github.com/edwingeng/deque"
	"math/rand"
	"io/ioutil"
)

const TIMEGRAN = 10

type IDUNNOMaster struct{
	WaitJobQueues	map[string]deque.Deque
	RunningJobQueues	map[string]deque.Deque
	ResourceTable	map[string]string
	IncarnationNum	int
	ModelList		[]Model
	ResourceMutex	sync.RWMutex
	IDMutex			sync.RWMutex
	WaitQMutex		sync.RWMutex
	RunningQMutex	sync.RWMutex
	ModelListMutex	sync.RWMutex
}

type Model struct {
	ModelName	string
	TimeList	[]int //may be time
	QueryRate	float
	BatchSize	int
}

var IDunnoMaster IDUNNOMaster

func InitIDunnoClient(){
	IDunnoMaster.WaitJobQueues = make(map[string]deque.Deque)
	IDunnoMaster.WaitJobQueues["resnet50"] = deque.NewDeque()
	IDunnoMaster.WaitJobQueues["resnet101"] = deque.NewDeque()
	IDunnoMaster.RunningJobQueues = make(map[string]deque.Deque)
	IDunnoMaster.RunningJobQueues["resnet50"] = deque.NewDeque()
	IDunnoMaster.RunningJobQueues["resnet101"] = deque.NewDeque()
	IDunnoMaster.ResourceTable = make(map[string]string)
	IDunnoMaster.ModelList = make([]Model)
	IDunnoMaster.IncarnationNum = 0
	resnet50 := Model{"resnet50", [], 0, 2};
	resnet101 := Model{"resnet101", [], 0, 2};
	IDunnoMaster.ModelList = append(IDunnoMaster.ModelList, resnet50)
	IDunnoMaster.ModelList = append(IDunnoMaster.ModelList, resnet101)

	if config.MyConfig.IsIntroducer() {
		go IDunnoMaster.ProcessQueryRequest()
	}
}

func GetRandomQuery(int num) []string {
	files, err := ioutil.ReadDir("/home/kc68/SDFS/dataset/")
    if err != nil {
        log.Fatal(err)
    }
	rand_permutation := rand.Perm(len(files))
	var res []string
	for i := 0; i < num; i++ {
		res = append(res, files[rand_permutation[i]].Name())
    }
	return res;
}

func (idunno *IDUNNOMaster) ProcessQueryRequest(){
	for _, m := range idunno.ModelList {
		queries := GetRandomQuery(m.BatchSize)
		idunno.WaitQMutex.Lock()
		for _, q := range queries {
			idunno.WaitJobQueues[m.ModelName].PushBack(q)
		}
		idunno.WaitQMutex.Unlock()
	}
	time.Sleep(TIMEGRAN * time.Second)
}

func (idunno *IDUNNOMaster) ShowWait(){
	for _, m := range idunno.ModelList {
		fmt.Printf("ModelName: %s", m.ModelName)
		for i, n := 0, idunno.WaitJobQueues[m.ModelName].Len(); i < n; i++ {
			tmp := idunno.WaitJobQueues[m.ModelName].PopFront()
			fmt.Println(tmp)
			idunno.WaitJobQueues[m.ModelName].PushBack(tmp)
		}
		fmt.Printf("\n")
	}
}

func (idunno *IDUNNOMaster) Schedule(){

}

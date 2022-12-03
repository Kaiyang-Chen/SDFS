package Sdfs

import (
	"fmt"
	// "net"
	// "net/rpc"
	"CS425MP2/config"
	"sync"
	"time"
	"github.com/edwingeng/deque"
	"math/rand"
	"log"
	"io/ioutil"
)

const TIMEGRAN = 10
const RES101 = "resnet101"
const RES50 = "resnet50"

type IDUNNOMaster struct{
	WaitJobQueues	map[string]deque.Deque
	RunningJobQueues	map[string]deque.Deque
	ResourceTable	map[string]string
	TriggerTime		map[string]map[string]time.Time
	IncarnationNum	int
	ModelList		map[string]Model
	ResourceMutex	sync.RWMutex
	IDMutex			sync.RWMutex
	WaitQMutex		sync.RWMutex
	RunningQMutex	sync.RWMutex
	ModelListMutex	sync.RWMutex
	TriggerMutex	sync.RWMutex
}

type Model struct {
	ModelName	string
	TimeList	[]float64 
	QueryRate	float64
	BatchSize	int
	count		int
	ModelMutex 	sync.RWMutex
}

var IDunnoMaster IDUNNOMaster

func InitIDunnoClient(){
	IDunnoMaster.WaitJobQueues = make(map[string]deque.Deque)
	IDunnoMaster.WaitJobQueues[RES50] = deque.NewDeque()
	IDunnoMaster.WaitJobQueues[RES101] = deque.NewDeque()
	IDunnoMaster.RunningJobQueues = make(map[string]deque.Deque)
	IDunnoMaster.RunningJobQueues[RES50] = deque.NewDeque()
	IDunnoMaster.RunningJobQueues[RES101] = deque.NewDeque()
	IDunnoMaster.ResourceTable = make(map[string]string)
	IDunnoMaster.TriggerTime = make(map[string]map[string]time.Time)
	IDunnoMaster.ModelList = make(map[string]Model)
	IDunnoMaster.IncarnationNum = 0
	IDunnoMaster.ModelList[RES50] = Model{RES50, make([]float64, 0), 0, 2, 0, sync.RWMutex{}}
	IDunnoMaster.ModelList[RES101] = Model{RES101, make([]float64, 0), 0, 2, 0, sync.RWMutex{}}

	if config.MyConfig.IsIntroducer() {
		go IDunnoMaster.ProcessQueryRequest()
		go IDunnoMaster.Scheduler()
	}
}

func GetRecentQueryRate(ModelName string) float64 {
	count := 0
	for _, t := range IDunnoMaster.TriggerTime[ModelName] {
		if time.Now().Before(t.Add(TIMEGRAN * time.Second)) {
			count++;
		}
	}
	return float64(count)/float64(TIMEGRAN)
}

func GetRandomQuery(num int) []string {
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
	for{
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
}

func (idunno *IDUNNOMaster) ShowWait(){
	for _, m := range idunno.ModelList {
		fmt.Println(m.ModelName)
		for i, n := 0, idunno.WaitJobQueues[m.ModelName].Len(); i < n; i++ {
			tmp := idunno.WaitJobQueues[m.ModelName].PopFront()
			fmt.Println(tmp)
			idunno.WaitJobQueues[m.ModelName].PushBack(tmp)
		}
		fmt.Printf("\n")
	}
}

func (idunno *IDUNNOMaster) ShowResourceTable() {
	for k, v := range idunno.ResourceTable {
		fmt.Printf("Node: %s\n", k)
		fmt.Printf("Task: %s\n", v)
	}
}

func (idunno *IDUNNOMaster) ShowRun(){
	for _, m := range idunno.ModelList {
		for i, n := 0, idunno.RunningJobQueues[m.ModelName].Len(); i < n; i++ {
			tmp := idunno.RunningJobQueues[m.ModelName].PopFront()
			fmt.Println(tmp)
			idunno.RunningJobQueues[m.ModelName].PushBack(tmp)
		}
		fmt.Printf("\n")
	}
}

func (idunno *IDUNNOMaster) Scheduler(){
	for {
		for node, task := range idunno.ResourceTable{
			if task == "" {
				if(!idunno.WaitJobQueues[RES50].Empty() || !idunno.WaitJobQueues[RES101].Empty()) {
					go idunno.DoScheduling(node)
				}
			}
		}
		time.Sleep(1 * time.Second)
	}
}

func (idunno *IDUNNOMaster) IncreaseIncarnationNum() {
	idunno.IDMutex.Lock()
	defer idunno.IDMutex.Unlock()
	idunno.IncarnationNum = idunno.IncarnationNum + 1
}

func (idunno *IDUNNOMaster) PushRunQ (model string, taskName string) {
	idunno.RunningQMutex.Lock()
	defer idunno.RunningQMutex.Unlock()
	idunno.RunningJobQueues[model].PushBack(taskName)
}

func (idunno *IDUNNOMaster) PopWaitQ (model string) string{
	idunno.WaitQMutex.Lock()
	defer idunno.WaitQMutex.Unlock()
	res, _ := idunno.WaitJobQueues[model].PopFront().(string)
	return res
}

func (idunno *IDUNNOMaster) PushTriggerT (model string, taskName string, timeStart time.Time) {
	idunno.TriggerMutex.Lock()
	defer idunno.TriggerMutex.Unlock()
	idunno.TriggerTime[RES101][taskName] = timeStart
}


func (idunno *IDUNNOMaster) ChangeResourceTable (targetAddr string, taskName string, delete bool) {
	idunno.ResourceMutex.Lock()
	defer idunno.ResourceMutex.Unlock()
	if(delete) {
		idunno.ResourceTable[targetAddr] = ""
	} else {
		idunno.ResourceTable[targetAddr] = taskName
	}
}

func (idunno *IDUNNOMaster) ChangeModelList (model string, timeStart time.Time, timeEnd time.Time) {
	idunno.ModelListMutex.Lock()
	defer idunno.ModelListMutex.Unlock()
	if entry, ok := idunno.ModelList[model]; ok {
		entry.TimeList = append(entry.TimeList, timeEnd.Sub(timeStart).Seconds())
		entry.count = entry.count + 1
		idunno.ModelList[model] = entry
	}
}





func (idunno *IDUNNOMaster) DoScheduling(targetAddr string) {
	fmt.Printf("DoScheduling!!\n")
	model := ""
	if(GetRecentQueryRate(RES50) > GetRecentQueryRate(RES101) && !idunno.WaitJobQueues[RES101].Empty()){
		model = RES101
	} else if (GetRecentQueryRate(RES101) > GetRecentQueryRate(RES50) && !idunno.WaitJobQueues[RES50].Empty()){
		model = RES50
	} else if (GetRecentQueryRate(RES101) == GetRecentQueryRate(RES50)){
		model = RES50
	}

	if model != "" {
		idunno.IncreaseIncarnationNum()
		queryName := idunno.PopWaitQ(model) 
		taskName := queryName + "-" + string(idunno.IncarnationNum)
		idunno.PushRunQ(model, taskName)
		timeStart := time.Now()
		idunno.PushTriggerT(model, taskName, timeStart)
		idunno.ChangeResourceTable(targetAddr, taskName, false)
		// rpc call task, error handling
		time.Sleep(5 * time.Second)
		// ------------
		
		timeEnd := time.Now()
		idunno.ChangeModelList(model, timeStart, timeEnd)
		idunno.ChangeResourceTable(targetAddr, taskName, true)
	}
}
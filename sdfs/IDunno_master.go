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
	"strconv"
	"strings"
)

const TIMEGRAN = 20
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
	IDunnoMaster.TriggerTime[RES50] = make(map[string]time.Time)
	IDunnoMaster.TriggerTime[RES101] = make(map[string]time.Time)
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
		if time.Now().Before(t.Add(2 * TIMEGRAN * time.Second)) {
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
	// fmt.Println(idunno.IncarnationNum)
}

func (idunno *IDUNNOMaster) PushRunQ (model string, taskName string, tail bool) {
	idunno.RunningQMutex.Lock()
	defer idunno.RunningQMutex.Unlock()
	if (tail) {
		idunno.RunningJobQueues[model].PushBack(taskName)
	} else {
		idunno.RunningJobQueues[model].PushFront(taskName)
	}
	
}

func (idunno *IDUNNOMaster) PushWaitQ (model string, taskName string, tail bool) {
	idunno.WaitQMutex.Lock()
	defer idunno.WaitQMutex.Unlock()
	if (tail) {
		idunno.WaitJobQueues[model].PushBack(taskName)
	} else {
		idunno.WaitJobQueues[model].PushFront(taskName)
	}
	
}

func (idunno *IDUNNOMaster) PopWaitQ (model string) string{
	idunno.WaitQMutex.Lock()
	defer idunno.WaitQMutex.Unlock()
	res, _ := idunno.WaitJobQueues[model].PopFront().(string)
	return res
}

func (idunno *IDUNNOMaster) PopRunQ (model string) string{
	idunno.RunningQMutex.Lock()
	defer idunno.RunningQMutex.Unlock()
	res, _ := idunno.RunningQMutex[model].PopFront().(string)
	return res
}

func (idunno *IDUNNOMaster) DeleteRunQ (model string, taskName string) {
	idunno.RunningQMutex.Lock()
	defer idunno.RunningQMutex.Unlock()
	for i, n := 0, idunno.RunningQMutex[model].Len(); i < n; i++ {
		tmp := idunno.WaitJobQueues[model].PopFront()
		if(tmp != taskName){
			idunno.WaitJobQueues[m.ModelName].PushBack(tmp)
		}
	}
}

func (idunno *IDUNNOMaster) PushTriggerT (model string, taskName string, timeStart time.Time) {
	idunno.TriggerMutex.Lock()
	defer idunno.TriggerMutex.Unlock()
	idunno.TriggerTime[model][taskName] = timeStart
}

func (idunno *IDUNNOMaster) DeleteTriggerT (model string, taskName string) {
	idunno.TriggerMutex.Lock()
	defer idunno.TriggerMutex.Unlock()
	delete(idunno.TriggerTime[model], taskName)
}


func (idunno *IDUNNOMaster) ChangeResourceTable (targetAddr string, taskName string, delete bool) string {
	idunno.ResourceMutex.Lock()
	defer idunno.ResourceMutex.Unlock()
	res := idunno.ResourceTable[targetAddr]
	if(delete) {
		idunno.ResourceTable[targetAddr] = ""
	} else {
		idunno.ResourceTable[targetAddr] = taskName
	}
	return res;
}


func (idunno *IDUNNOMaster) DeleteResourceTable (targetAddr string, taskName string) string {
	idunno.ResourceMutex.Lock()
	defer idunno.ResourceMutex.Unlock()
	res := idunno.ResourceTable[targetAddr]
	delete(idunno.ResourceTable, targetAddr)	
	return res;
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

func (idunno *IDUNNOMaster) HandleLeaving(addr string) {
	// RunningQMutex.Lock()
	// TriggerMutex.Lock()
	// ResourceMutex.Lock()
	// defer RunningQMutex.Unlock()
	// defer TriggerMutex.Unlock()
	// defer ResourceMutex.Unlock()
	for _, m := range idunno.ModelList{
		taskName := idunno.DeleteResourceTable(addr, taskName)
		idunno.DeleteRunQ(m.ModelName, taskName)
		queryName := strings.Split(taskName, "-")[0]
		idunno.PushWaitQ(m.ModelName, queryName, false)
		idunno.DeleteTriggerT(m.ModelName, taskName)
	}
}



func (idunno *IDUNNOMaster) DoScheduling(targetAddr string) {
	// fmt.Printf("DoScheduling!!\n")
	model := ""
	if(GetRecentQueryRate(RES50) > GetRecentQueryRate(RES101) && !idunno.WaitJobQueues[RES101].Empty()){
		model = RES101
	} else if (GetRecentQueryRate(RES101) > GetRecentQueryRate(RES50) && !idunno.WaitJobQueues[RES50].Empty()){
		model = RES50
	} else if (GetRecentQueryRate(RES101) == GetRecentQueryRate(RES50)){
		model = RES50
	}
	fmt.Println(model)
	if model != "" {
		idunno.IncreaseIncarnationNum()
		queryName := idunno.PopWaitQ(model) 
		taskName := queryName + "-" + strconv.Itoa(idunno.IncarnationNum) + model
		idunno.PushRunQ(model, taskName, true)
		timeStart := time.Now()
		idunno.PushTriggerT(model, taskName, timeStart)
		idunno.ChangeResourceTable(targetAddr, taskName, false)
		// rpc call task, error handling
		time.Sleep(10 * time.Second)
		// ------------
		
		timeEnd := time.Now()
		idunno.ChangeModelList(model, timeStart, timeEnd)
		idunno.ChangeResourceTable(targetAddr, taskName, true)
	}
}


// 1. handel leaving
// 2. copy IDunno master info, hot update, if change master, undo all the working task
// 3. rpc for inference
// 4. cmdline
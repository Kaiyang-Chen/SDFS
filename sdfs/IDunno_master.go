package Sdfs

import (
	"fmt"
	"net/rpc"
	"CS425MP2/config"
	"sync"
	"time"
	"github.com/edwingeng/deque"
	"math/rand"
	"log"
	"io/ioutil"
	"strconv"
	"strings"
	"os/exec"
	"github.com/montanaflynn/stats"
)

const TIMEGRAN = 10
const WAITQSIZE = 5
const RES101 = "resnet101"
const RES50 = "resnet50"

type Args struct {
	InputPath   string
	OutputPath  string
	ModelPath  string
}

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
	Count		int
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
	go IDunnoMaster.RegisterRpc()
}


func GetRecentQueryRate(ModelName string) float64 {
	IDunnoMaster.ModelListMutex.Lock()
	defer IDunnoMaster.ModelListMutex.Unlock()
	count := 0
	for _, t := range IDunnoMaster.TriggerTime[ModelName] {
		if time.Now().Before(t.Add(TIMEGRAN * time.Second)) {
			count++;
		}
	}
	if entry, ok := IDunnoMaster.ModelList[ModelName]; ok {
		entry.QueryRate = float64(count)/float64(TIMEGRAN)
		IDunnoMaster.ModelList[ModelName] = entry
	}

	return float64(count)/float64(TIMEGRAN)
}


func (idunno *IDUNNOMaster) GetTruncatedModelList() map[string]Model{
	tmpModelList := make(map[string]Model)
	for _, m := range idunno.ModelList {
		timeList := m.TimeList
		if(len(timeList)>20){
			timeList = timeList[len(timeList)-20:]
		}
		tmpModelList[m.ModelName] = Model{m.ModelName, timeList, m.QueryRate, m.BatchSize, m.Count,sync.RWMutex{}}
	}
	return tmpModelList
}

func (idunno *IDUNNOMaster) GetLatestTrigger() map[string]map[string]time.Time {
	tmpTrigger := make(map[string]map[string]time.Time)
	tmpTrigger[RES50] = make(map[string]time.Time)
	tmpTrigger[RES101] = make(map[string]time.Time)
	for _, m := range idunno.ModelList {
		for task, t := range IDunnoMaster.TriggerTime[m.ModelName] {
			if time.Now().Before(t.Add(TIMEGRAN * time.Second)) {
				tmpTrigger[m.ModelName][task] = t;
			}
		}
	}
	return tmpTrigger
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

func (idunno *IDUNNOMaster) C1() {
	for _, m := range idunno.ModelList {
		fmt.Println(m.ModelName)
		fmt.Println(GetRecentQueryRate(m.ModelName))
		fmt.Println(m.Count)
		fmt.Printf("\n")
	}
}

func (idunno *IDUNNOMaster) C2() {
	for _, m := range idunno.ModelList {
		fmt.Println(m.ModelName)
		list := m.TimeList
		mean, _ := stats.Mean(list)
		median, _ := stats.Median(list)
		std, _ := stats.StandardDeviation(list)
		p90, _ := stats.Percentile(list, 90)
		p95, _ := stats.Percentile(list, 95)
		p99, _ := stats.Percentile(list, 99)
		fmt.Printf("Mean: %f; Median: %f; STD: %f; P90: %f; P95: %f; P99: %f; \n", mean, median, std, p90, p95, p99)
	}
}

func (idunno *IDUNNOMaster) C3(model string, batchSize int) {
	idunno.ModelListMutex.Lock()
	defer idunno.ModelListMutex.Unlock()
	if entry, ok := idunno.ModelList[model]; ok {
		entry.BatchSize = batchSize
		idunno.ModelList[model] = entry
	}
}

func (idunno *IDUNNOMaster) C4(taskName string) {
	sdfsName := taskName + "_out"
	// SdfsClient.GetFile("/home/kc68/files/"+sdfsName, sdfsName)
	cmd := exec.Command("cat", "/home/kc68/files/"+sdfsName)
	res, err := cmd.CombinedOutput()

	if err != nil {
		fmt.Println(err)
	} else {
		fmt.Println(string(res))
	}
}

func (idunno *IDUNNOMaster) ProcessQueryRequest(){
	for{
		for _, m := range idunno.ModelList {
			if(idunno.WaitJobQueues[m.ModelName].Len() >= WAITQSIZE){
				continue
			}

			queries := GetRandomQuery((m.BatchSize+idunno.WaitJobQueues[m.ModelName].Len())%WAITQSIZE)
			idunno.WaitQMutex.Lock()
			for _, q := range queries {
				// fmt.Println(q)
				idunno.WaitJobQueues[m.ModelName].PushBack(q)
			}
			idunno.WaitQMutex.Unlock()
		}
		time.Sleep(TIMEGRAN * time.Second)
	}
}

func (idunno *IDUNNOMaster) List2Deque(dict map[string][]string) map[string]deque.Deque {
	res := make(map[string]deque.Deque)
	res[RES50] = deque.NewDeque()
	res[RES101] = deque.NewDeque()
	for model, q:= range dict {
		for _, task := range q {
			res[model].PushBack(task)
		}
	}
	return res
}

func (idunno *IDUNNOMaster) ShowWait(show bool)map[string][]string{
	res := make(map[string][]string)
	for _, m := range idunno.ModelList {
		if show {
			fmt.Println(m.ModelName)
		}
		var tmpQ []string
		for i, n := 0, idunno.WaitJobQueues[m.ModelName].Len(); i < n; i++ {
			tmp, _ := idunno.WaitJobQueues[m.ModelName].PopFront().(string)
			// tmp := idunno.WaitJobQueues[m.ModelName].PopFront()
			if show {
				fmt.Println(tmp)
			}
			tmpQ = append(tmpQ, tmp)
			idunno.WaitJobQueues[m.ModelName].PushBack(tmp)
		}
		res[m.ModelName] = tmpQ
		// fmt.Printf("\n")
	}
	return res;
}

func (idunno *IDUNNOMaster) ShowResourceTable() {
	for k, v := range idunno.ResourceTable {
		fmt.Printf("Node: %s\n", k)
		fmt.Printf("Task: %s\n", v)
	}
}

func (idunno *IDUNNOMaster) ShowRun(show bool) map[string][]string{
	res := make(map[string][]string)
	for _, m := range idunno.ModelList {
		if show {
			fmt.Println(m.ModelName)
		}
		var tmpQ []string
		for i, n := 0, idunno.RunningJobQueues[m.ModelName].Len(); i < n; i++ {
			tmp, _ := idunno.RunningJobQueues[m.ModelName].PopFront().(string)
			// tmp := idunno.RunningJobQueues[m.ModelName].PopFront()
			if show {
				fmt.Println(tmp)
			}
			tmpQ = append(tmpQ, tmp)
			idunno.RunningJobQueues[m.ModelName].PushBack(tmp)
		}
		res[m.ModelName] = tmpQ
		// fmt.Printf("\n")
	}
	return res;
}


func (idunno *IDUNNOMaster) Scheduler(){
	for {
		tmp := idunno.ResourceTable
		for node, task := range tmp{
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
	res, _ := idunno.RunningJobQueues[model].PopFront().(string)
	return res
}

func (idunno *IDUNNOMaster) DeleteRunQ (model string, taskName string) {
	idunno.RunningQMutex.Lock()
	defer idunno.RunningQMutex.Unlock()
	for i, n := 0, idunno.RunningJobQueues[model].Len(); i < n; i++ {
		tmp := idunno.RunningJobQueues[model].PopFront()
		if(tmp != taskName){
			idunno.RunningJobQueues[model].PushBack(tmp)
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


func (idunno *IDUNNOMaster) DeleteResourceTable (targetAddr string) string {
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
		entry.Count = entry.Count + 1
		idunno.ModelList[model] = entry
	}
}

func (idunno *IDUNNOMaster) HandleLeaving(addr string) {
	taskName := idunno.DeleteResourceTable(addr)
	if (taskName != "") {
		queryName := strings.Split(taskName, "-")[0]
		model := strings.Split(taskName, "-")[2]
		idunno.RollBackQuery(model, queryName, taskName)
	}
}


func (idunno *IDUNNOMaster) RollBackRunq() {
	for _, m := range idunno.ModelList {
		for i, n := 0, idunno.RunningJobQueues[m.ModelName].Len(); i < n; i++ {
			taskName, _ := idunno.RunningJobQueues[m.ModelName].PopFront().(string)
			queryName := strings.Split(taskName, "-")[0]
			idunno.RollBackQuery(m.ModelName, queryName, taskName)
		}
	}
}

func (idunno *IDUNNOMaster) GetComputeNode(taskName string) string {
	for k, v := range idunno.ResourceTable {
		if v == taskName {
			return k
		}
	}
	return ""
}

func (idunno *IDUNNOMaster) RollBackQuery(model string, queryName string, taskName string) {
	idunno.DeleteRunQ(model, taskName)
	idunno.PushWaitQ(model, queryName, false)
	idunno.DeleteTriggerT(model, taskName)
	targetAddr := idunno.GetComputeNode(taskName)
	if(targetAddr != "") {
		idunno.ChangeResourceTable(targetAddr, taskName, true)
	}
}


func (idunno *IDUNNOMaster) DoInference(targetAddr string, model string, queryName string, taskName string) error {
	var modelPath string
	if(model == RES50) {
		modelPath = "/home/kc68/SDFS/resnet50.py"
	}else{
		modelPath = "/home/kc68/SDFS/resnet101.py"
	}
	args := Args{"/home/kc68/SDFS/dataset/"+queryName, "/home/kc68/files/"+taskName+"_out", modelPath}
	client, err := rpc.Dial("tcp", targetAddr)
	if err != nil {
		log.Println("dialing:", err)
		return err
	}
	defer func() {
		if client != nil {
			client.Close()
		}
	}()
	var inferenceResult string
	err = client.Call("InferenceService.Inference", args, &inferenceResult)
	if err != nil {
		log.Println(err)
	}else{
		fmt.Printf("Task %s done on machine %s.\n",taskName, targetAddr)
	}
	// else {
	// 	fmt.Println(inferenceResult)
	// }
	return err
}



func (idunno *IDUNNOMaster) DoScheduling(targetAddr string) {
	// fmt.Printf("DoScheduling!!\n")
	model := ""
	if(GetRecentQueryRate(RES50) > GetRecentQueryRate(RES101) && !idunno.WaitJobQueues[RES101].Empty()){
		model = RES101
	} else if (GetRecentQueryRate(RES101) > GetRecentQueryRate(RES50) && !idunno.WaitJobQueues[RES50].Empty()){
		model = RES50
	} else if (GetRecentQueryRate(RES101) == GetRecentQueryRate(RES50)){
		if(!idunno.WaitJobQueues[RES101].Empty()){
			model = RES101
		} else if(!idunno.WaitJobQueues[RES50].Empty()){
			model = RES50
		}

	}
	// fmt.Println(model)
	if model != "" {
		idunno.IncreaseIncarnationNum()
		queryName := idunno.PopWaitQ(model) 
		taskName := queryName + "-" + strconv.Itoa(idunno.IncarnationNum) + "-" + model
		idunno.PushRunQ(model, taskName, true)
		timeStart := time.Now()
		idunno.PushTriggerT(model, taskName, timeStart)
		idunno.ChangeResourceTable(targetAddr, taskName, false)

		// time.Sleep(6 * time.Second)
		// ------------
		err := idunno.DoInference(targetAddr, model, queryName, taskName)
		if err != nil {
			fmt.Println("inference err:", err)
			idunno.RollBackQuery(model, queryName, taskName)
			return
		} 
		// else {
		// 	fmt.Printf("Query %s finished.\n", taskName)
		// }
		
		timeEnd := time.Now()
		idunno.ChangeModelList(model, timeStart, timeEnd)
		idunno.ChangeResourceTable(targetAddr, taskName, true)
		idunno.DeleteRunQ(model, taskName)
	}
}

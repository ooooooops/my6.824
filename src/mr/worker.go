package mr

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	combinef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for {
		should_break := WorkerDo(mapf, reducef, combinef)
		if should_break {
			break
		}
	}
}

func WorkerDo(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
	combinef func(string, []string) string) bool {
	log.Println("[WORKER] try to get task...")
	taskinfo := GetOneTask()

	if taskinfo.TaskNum < 0 {
		return true
	}
	if taskinfo.TaskType == 0 {
		// 没有任务的时候睡10s以免短时间发起大量无用的rpc
		time.Sleep(time.Second * 10)
		/*
			当前暂时没有任务，比如所有map都在运行中的情况下再请求新任务就没有新任务了。
			这个时候worker需要在下一个循环中检测有没有新任务
		*/
		return false
	}
	if taskinfo.TaskType == 1 {
		log.Println("[WORKER] processing map task...")
		WorkerDoMap(mapf, combinef, taskinfo)
	} else {
		log.Println("[WORKER] processing reduce task...")
		WorkerDoReduce(reducef, taskinfo)
	}
	// worker继续运行以接受下一个map或reduce任务
	return false
}

// 传入reducef是为了做combination
func WorkerDoMap(mapf func(string, string) []KeyValue,
	combinef func(string, []string) string, taskinfo TaskInfo) {
	filename := taskinfo.FileName
	// log.Printf("[MAPPER] mapper processing file %s\n", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	// log.Printf("[MAPPER] calling user map function")
	intermediate := mapf(filename, string(content))
	fileMap := make(map[string]*os.File)
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		// combination
		j := i + 1
		var output string
		if combinef != nil {
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			output = combinef(intermediate[i].Key, values)
		} else {
			output = intermediate[i].Value
		}

		// partition
		// 对key做hash并对nReduce取模得到reduce编号Y，把outputx写入文件mr-X-Y
		KeyHash := ihash(intermediate[i].Key)
		ReducerNum := KeyHash % taskinfo.ReduceNum // 当前key应该分发给第ReducerNum个Reduce

		oname := "mr-" + strconv.Itoa(taskinfo.TaskNum) + "-" + strconv.Itoa(ReducerNum)
		ofile, exists := fileMap[oname]
		if !exists {
			ofile, _ = os.Create(oname)
			fileMap[oname] = ofile
		}

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		// log.Printf("[MAPPER] write %v %v to file(%v)\n", intermediate[i].Key, output, ofile.Name())
		i = j
	}

	files := make([]string, 0, len(fileMap))
	for fname, fhandle := range fileMap {
		fhandle.Close()
		// log.Printf("[MAPPER] file %s closed", fname)
		files = append(files, fname)
	}

	SendMapCompleteMsg(files, taskinfo.TaskNum)
	// log.Printf("[MAPPER] SendMapCompleteMsg done")
}

func SendMapCompleteMsg(files []string, task_num int) {
	request := MapCompleteRequest{task_num, files}
	reply := MapCompleteReply{}
	call("Master.MapTaskComplete", &request, &reply)
}

func WorkerDoReduce(reducef func(string, []string) string, taskinfo TaskInfo) {
	// 从taskinfo.PathList给出的目录中读出所有的mr-*-{taskinfo.Num}文件中的kv pair
	kva := []KeyValue{}
	for _, filePath := range taskinfo.PathList {
		// log.Printf("[REDUCE] found file %s in reducer(%d)", filePath, taskinfo.TaskNum)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open %v", filePath)
		}
		scanner := bufio.NewScanner(file)
		for scanner.Scan() {
			line := scanner.Text()
			kv := strings.SplitN(line, " ", 2)
			kva = append(kva, KeyValue{kv[0], kv[1]})
		}
		file.Close()
	}
	// 当前reduce需要处理的所有kv都在kva里面
	// 为了防止hash碰撞导致的错误（不能认为kva的所有key都相同），先排序再处理
	sort.Sort(ByKey(kva))
	oname := "mr-out-" + strconv.Itoa(taskinfo.TaskNum)
	ofile, _ := os.Create(oname)
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		// log.Printf("[REDUCER] write %v %v to reducef\n", kva[i].Key, values)
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
		// log.Printf("[REDUCER] write %s %s to file(%s)\n", kva[i].Key, output, oname)
		i = j
	}
	ofile.Close()

	SendReduceCompleteMsg(oname, taskinfo.TaskNum)
}

func SendReduceCompleteMsg(file string, task_num int) {
	request := ReduceCompleteRequest{task_num, file}
	reply := ReduceCompleteReply{}
	call("Master.ReduceTaskComplete", &request, &reply)
}

func GetOneTask() TaskInfo {
	request := TaskRequest{}
	// reply := TaskReply{99, 888, 0, "", nil} // 任务号<0表示异常
	reply := new(TaskReply)
	// log.Printf("1recvd task:num(%d),type(%d),nreduce(%d),map file(%s)", reply.TaskNum, reply.TaskType, reply.ReduceNum, reply.FileName)
	no_error := call("Master.BuildTask", &request, &reply)
	// log.Printf("[WORKER] recvd task:num(%d),type(%d),nreduce(%d),map file(%s)", reply.TaskNum, reply.TaskType, reply.ReduceNum, reply.FileName)
	if !no_error {
		log.Printf("[WORKER] master exited")
		return *reply // 返回nil认为master已经退出。TODOYYJ 比较好的做法是worker启动以后向master注册，master退出后通知worker退出
	}
	return *reply
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()
	// fmt.Printf("[%s]1\n", rpcname)
	err = c.Call(rpcname, args, reply)
	if err == nil {
		// fmt.Printf("2[%s] request[%v] reply[%v]\n", rpcname, args, reply)
		return true
	}
	// fmt.Printf("3[%s] request[%v] reply[%v]\n", rpcname, args, reply)
	fmt.Println(err)
	return false
}

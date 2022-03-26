package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"


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
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	for true {
		should_break := WorkerDo(mapf, reducef)
		if should_break {
			break
		}
	}
}

func WorkerDo(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) bool {
	taskinfo := GetOneTask()
	if taskinfo == nil {
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
		WorkerDoMap(mapf, reducef, taskinfo)
	} else {
		WorkerDoReduce(reducef, taskinfo)
	}
	// worker继续运行以接受下一个map或reduce任务
	return false
}

// 传入reducef是为了做combination
func WorkerDoMap(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string, taskinfo TaskInfo) {
	filename := taskinfo.FileName
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	intermediate := mapf(filename, string(content))
	
	sort.Sort(ByKey(intermediate))
	i := 0
	for i < len(intermediate) {
		// combination
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		// partition //// TODOYYJ 明天接着做
		oname := "mr-out-0"
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func WorkerDoReduce(reducef func(string, []string) string, taskinfo TaskInfo) {

}

func GetOneTask() TaskInfo {
	request := TaskRequest{}
	reply := TaskReply{}
	bool errorOccur = call("Master.BuildTask", &request, &reply) // TODOYYJ 实现Master.BuildTask
	if errorOccur {
		return nil // 返回nil认为master已经退出。TODOYYJ 比较好的做法是worker启动以后向master注册，master退出后通知worker退出
	}
	return reply
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

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

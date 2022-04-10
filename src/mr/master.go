package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
)

const (
	TASK_STATUS_INIT = iota
	TASK_STATUS_DISPATCHED
	TASK_STATUS_FAILED
	TASK_STATUS_SUCCEED
)

type MapTaskInfo struct {
	Num        int // 任务编号
	FilePath   string
	TaskStatus int
}

type ReduceTaskInfo struct {
	Num        int // 任务编号
	PathList   []string
	ResultFile string
	TaskStatus int
}

type Master struct {
	// Your definitions here.
	Files   []string
	NReduce int

	MapTasks    []MapTaskInfo
	ReduceTasks []ReduceTaskInfo
	TaskStatus  int

	mutex sync.Mutex
}

func (m *Master) InitTasks(files []string, nReduce int) {
	for i := 0; i < len(files); i++ {
		map_info := MapTaskInfo{i, files[i], TASK_STATUS_INIT}
		m.MapTasks = append(m.MapTasks, map_info)
		log.Printf("master will process file [%s]\n", files[i])
	}

	for i := 0; i < nReduce; i++ {
		reduce_info := ReduceTaskInfo{i, nil, "", TASK_STATUS_INIT}
		m.ReduceTasks = append(m.ReduceTasks, reduce_info)
	}
	log.Printf("master will startup %d reducers\n", nReduce)

	m.TaskStatus = TASK_STATUS_INIT

	log.Println()
}

// TODOYYJ
// 1. 任务超时重置
// 2. worker向master发起请求的超时机制
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) BuildTask(args *TaskRequest, reply *TaskReply) error {
	log.Printf("master recv task request")
	for i := 0; i < len(m.MapTasks); i++ {
		m.mutex.Lock()
		if m.MapTasks[i].TaskStatus == TASK_STATUS_INIT || m.MapTasks[i].TaskStatus == TASK_STATUS_FAILED {
			m.MapTasks[i].TaskStatus = TASK_STATUS_DISPATCHED
			m.mutex.Unlock()
			reply.Num = m.MapTasks[i].Num
			reply.TaskType = 1
			reply.ReduceNum = len(m.ReduceTasks)
			reply.FileName = m.MapTasks[i].FilePath
			reply.PathList = nil
			log.Printf("send task:num(%d),type(%d),nreduce(%d),map file(%s)", reply.Num, reply.TaskType, reply.ReduceNum, reply.FileName)
			return nil
		}
		m.mutex.Unlock()
	}
	// 没有找到map任务，检查所有map任务是否完成；如果没有完成则等待，完成则分配reduce任务
	map_finished := true
	for {
		for i := 0; i < len(m.MapTasks); i++ {
			if m.MapTasks[i].TaskStatus != TASK_STATUS_SUCCEED {
				map_finished = false
				break
			}
		}
		if map_finished { // 等待所有map完成
			break
		}
	}
	// 分配reduce任务
	for i := 0; i < len(m.ReduceTasks); i++ {
		m.mutex.Lock()
		if m.ReduceTasks[i].TaskStatus == TASK_STATUS_INIT || m.ReduceTasks[i].TaskStatus == TASK_STATUS_FAILED {
			m.ReduceTasks[i].TaskStatus = TASK_STATUS_DISPATCHED
			m.mutex.Unlock()
			reply.Num = m.ReduceTasks[i].Num
			reply.TaskType = 2
			reply.ReduceNum = len(m.ReduceTasks)
			reply.FileName = ""
			reply.PathList = m.ReduceTasks[i].PathList
			return nil
		}
		m.mutex.Unlock()
	}

	m.TaskStatus = TASK_STATUS_SUCCEED
	return nil
}

func (m *Master) MapTaskComplete(args *MapCompleteRequest, reply *MapCompleteReply) error {
	if args.Num >= len(m.MapTasks) {
		log.Fatalf("params error")
		return nil
	}
	if m.MapTasks[args.Num].TaskStatus == TASK_STATUS_SUCCEED { // 防止超时任务突然返回
		return nil
	}
	m.MapTasks[args.Num].TaskStatus = TASK_STATUS_SUCCEED
	files := args.ResultPath
	for i := 0; i < len(files); i++ {
		split := strings.Split(files[i], "-")
		reduce_num, _ := strconv.Atoi(split[2])
		m.mutex.Lock()
		m.ReduceTasks[reduce_num].PathList = append(m.ReduceTasks[reduce_num].PathList, files[i])
		m.mutex.Unlock()
	}
	return nil
}

func (m *Master) ReduceTaskComplete(args *ReduceCompleteRequest, reply *ReduceCompleteReply) error {
	if m.ReduceTasks[args.Num].TaskStatus == TASK_STATUS_SUCCEED { // 防止超时任务突然返回
		return nil
	}
	m.ReduceTasks[args.Num].TaskStatus = TASK_STATUS_SUCCEED
	m.ReduceTasks[args.Num].ResultFile = args.ResultPath
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	ret = m.TaskStatus == TASK_STATUS_SUCCEED

	return ret
}

func CheckParams() bool {
	// TODOYYJ
	return true
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.Files = files
	m.NReduce = nReduce
	m.InitTasks(files, nReduce)
	m.server()
	return &m
}

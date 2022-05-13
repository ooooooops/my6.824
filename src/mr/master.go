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
	"time"
)

const (
	TASK_STATUS_INIT = iota
	TASK_STATUS_DISPATCHED
	TASK_STATUS_FAILED
	TASK_STATUS_TIMEDOUT
	TASK_STATUS_SUCCEED
)

type MapTaskInfo struct {
	Num          int // 任务编号
	FilePath     string
	TaskStatus   int
	DispatchTime int
}

type ReduceTaskInfo struct {
	Num          int // 任务编号
	PathList     []string
	ResultFile   string
	TaskStatus   int
	DispatchTime int
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
		map_info := MapTaskInfo{i, files[i], TASK_STATUS_INIT, 0}
		m.MapTasks = append(m.MapTasks, map_info)
		log.Printf("master will process file [%s]\n", files[i])
	}

	for i := 0; i < nReduce; i++ {
		reduce_info := ReduceTaskInfo{i, nil, "", TASK_STATUS_INIT, 0}
		m.ReduceTasks = append(m.ReduceTasks, reduce_info)
	}
	log.Printf("master will startup %d reducers\n", nReduce)

	m.TaskStatus = TASK_STATUS_INIT

	log.Println()
}

// 2. worker向master发起请求的超时机制
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) ResetTimedoutTask() {
	for {
		time_now := time.Now().Second()
		for i := 0; i < len(m.MapTasks); i++ {
			m.mutex.Lock()
			if m.MapTasks[i].TaskStatus == TASK_STATUS_DISPATCHED {
				time_elapse := time_now - m.MapTasks[i].DispatchTime
				if time_elapse > 10 {
					m.MapTasks[i].TaskStatus = TASK_STATUS_TIMEDOUT
					log.Printf("[MASTER] map[%d] timed out, reset", m.MapTasks[i].Num)
				}
			}
			m.mutex.Unlock()
		}

		for i := 0; i < len(m.ReduceTasks); i++ {
			m.mutex.Lock()
			if m.ReduceTasks[i].TaskStatus == TASK_STATUS_DISPATCHED {
				time_elapse := time_now - m.ReduceTasks[i].DispatchTime
				if time_elapse > 10 {
					m.ReduceTasks[i].TaskStatus = TASK_STATUS_TIMEDOUT
					log.Printf("[MASTER] reduce[%d] timed out, reset", m.ReduceTasks[i].Num)
				}
			}
			m.mutex.Unlock()
		}
	}
}

func (m *Master) BuildMapTask(args *TaskRequest, reply *TaskReply) bool {
	for i := 0; i < len(m.MapTasks); i++ {
		m.mutex.Lock()
		if m.MapTasks[i].TaskStatus == TASK_STATUS_INIT || m.MapTasks[i].TaskStatus == TASK_STATUS_TIMEDOUT ||
			m.MapTasks[i].TaskStatus == TASK_STATUS_FAILED {
			m.MapTasks[i].TaskStatus = TASK_STATUS_DISPATCHED
			m.MapTasks[i].DispatchTime = time.Now().Second()
			m.mutex.Unlock()

			// log.Printf("1send task:num(%d),type(%d),nreduce(%d),map file(%s)", reply.TaskNum, reply.TaskType, reply.ReduceNum, reply.FileName)
			// reply.TaskNum = 1
			reply.TaskNum = m.MapTasks[i].Num
			reply.TaskType = 1
			reply.ReduceNum = len(m.ReduceTasks)
			reply.FileName = m.MapTasks[i].FilePath
			reply.PathList = nil
			log.Printf("[MASTER] send map:num(%d),type(%d),nreduce(%d),map file(%s)", reply.TaskNum, reply.TaskType, reply.ReduceNum, reply.FileName)
			return true
		}
		m.mutex.Unlock()
	}
	return false
}

func (m *Master) BuildReduceTask(args *TaskRequest, reply *TaskReply) bool {
	for i := 0; i < len(m.ReduceTasks); i++ {
		m.mutex.Lock()
		if m.ReduceTasks[i].TaskStatus == TASK_STATUS_INIT || m.ReduceTasks[i].TaskStatus == TASK_STATUS_TIMEDOUT ||
			m.ReduceTasks[i].TaskStatus == TASK_STATUS_FAILED {
			m.ReduceTasks[i].TaskStatus = TASK_STATUS_DISPATCHED
			m.ReduceTasks[i].DispatchTime = time.Now().Second()
			m.mutex.Unlock()
			reply.TaskNum = m.ReduceTasks[i].Num
			reply.TaskType = 2
			reply.ReduceNum = len(m.ReduceTasks)
			reply.FileName = ""
			reply.PathList = m.ReduceTasks[i].PathList
			log.Printf("[MASTER] send reduce:num(%d),type(%d),nreduce(%d),map file(%s)", reply.TaskNum, reply.TaskType, reply.ReduceNum, reply.FileName)
			return true
		}
		m.mutex.Unlock()
	}
	return false
}

func (m *Master) BuildTask(args *TaskRequest, reply *TaskReply) error {
	log.Printf("[MASTER] recv task request")
	build_map_success := m.BuildMapTask(args, reply)
	if build_map_success {
		return nil
	}
	// 没有找到map任务，检查所有map任务是否完成；如果没有完成则等待，完成则分配reduce任务
	for {
		map_finished := true
		for i := 0; i < len(m.MapTasks); i++ {
			if m.MapTasks[i].TaskStatus != TASK_STATUS_SUCCEED {
				log.Printf("[MASTER] map(%d) status(%d)", m.MapTasks[i].Num, m.MapTasks[i].TaskStatus)
				map_finished = false
				break
			}
		}
		if map_finished { // 等待所有map完成
			break
		}
		build_map_success := m.BuildMapTask(args, reply)
		if build_map_success {
			return nil
		}
		log.Printf("[MASTER] wait all map finish...")
		time.Sleep(time.Second * 1)
	}
	log.Printf("[MASTER] map tasks finished, starting dispathing reduce tasks...")
	// 分配reduce任务
	build_reduce_success := m.BuildReduceTask(args, reply)
	if build_reduce_success {
		return nil
	}

	for {
		reduce_finished := true
		for i := 0; i < len(m.ReduceTasks); i++ {
			if m.ReduceTasks[i].TaskStatus != TASK_STATUS_SUCCEED {
				reduce_finished = false
				break
			}
		}
		if reduce_finished { // 等待所有reduce完成
			break
		}
		build_reduce_success := m.BuildReduceTask(args, reply)
		if build_reduce_success {
			return nil
		}
		log.Printf("[MASTER] wait all reduce finish...")
		time.Sleep(time.Second * 1)
	}
	log.Printf("[MASTER] all tasks finished")
	m.TaskStatus = TASK_STATUS_SUCCEED
	return nil
}

func (m *Master) MapTaskComplete(args *MapCompleteRequest, reply *MapCompleteReply) error {
	log.Printf("[MASTER] MapTaskComplete")
	if args.Num >= len(m.MapTasks) {
		log.Fatalf("params error")
		// log.Printf("params error")
		return nil
	}
	if m.MapTasks[args.Num].TaskStatus == TASK_STATUS_SUCCEED ||
		m.MapTasks[args.Num].TaskStatus == TASK_STATUS_TIMEDOUT { // 防止超时任务突然返回
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
	log.Printf("[MASTER] ReduceTaskComplete")
	if m.ReduceTasks[args.Num].TaskStatus == TASK_STATUS_SUCCEED ||
		m.ReduceTasks[args.Num].TaskStatus == TASK_STATUS_TIMEDOUT { // 防止超时任务突然返回
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
	go m.ResetTimedoutTask()
	m.server()
	return &m
}

package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

/*********Map相关*************/
// worker向master发起任务请求
type TaskRequest struct {
}

// master向worker返回任务参数
type TaskReply struct {
	Num       int      // 任务编号
	TaskType  int      // 0 - NONE 无效任务, 1- Map, 2 - Reduce
	ReduceNum int      // Reduce任务的数量
	FileName  string   // map任务使用，指向具体的文件
	PathList  []string // Reduce任务使用，目录列表，worker需要根据reduce任务编号去每个目录下寻找名为mr-*-{Num}的文件
}

type TaskInfo = TaskReply

// worker完成map任务后向master返回结果路径
type MapCompleteRequest struct {
	Num        int
	ResultPath []string
}

// master响应MapCompleteRequest
type MapCompleteReply struct {
}

/*********Reduce相关*************/
// worker向master发起任务请求
// type TaskRequest struct {

// }

// type TaskReply struct {

// }

// worker完成reduce任务后向master返回结果路径
type ReduceCompleteRequest struct {
	Num        int
	ResultPath string
}

// master响应ReduceCompleteRequest
type ReduceCompleteReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

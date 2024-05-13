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

// example to show how to declare the arguments
// and reply for an RPC.
type ExampleArgs struct {
	X int
}
type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

// def for worker to communicate with coordinator on which job type to do
// no need to pass in args
type GetTaskArgs struct {
}

// the info returned by rpc from the coordinator (the data that worker needs to do further processes)
type GetTaskReply struct {
	Filename string
	Index    int
	JobType  string
	NReduce  int
	NMap  	 int
}

// need to pass the file name for update
type MapUpStatusArgs struct {
	Filename string
	Index    int
}

// no need to get anything back
type MapUpStatusReply struct {
}

// need to pass id for update
type ReduceUpStatusArgs struct {
	Index    int
}

// no need to get anything back
type ReduceUpStatusReply struct {
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}

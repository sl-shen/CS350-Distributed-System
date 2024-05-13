package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


// use constant to mark status
const (
	ready = iota
	started
	end
)

// create a struct to contain info for single task
type MapTask struct { // may change
	filename string
	index    int
	jobType  string
	status   int
}

var logger *log.Logger

func init() {
    logger = log.New(os.Stdout, "INFO: ", log.Ldate|log.Ltime|log.Lshortfile)
}

// create var for map channel
var mapChan chan MapTask
// create var for reduce channel
var reduceChan chan int

type Coordinator struct {
	// Your definitions here.

	// a hashmap to track map status for each file, filename -> MapTask
	mapTrack 		map[string]MapTask
	// a hashmap to track reduce status for each partition, partition id -> MapTask
	reduceTrack 	map[int]MapTask
	// a flag to show the map phase finishes, and need to start to do reduce
	finishMap 		bool
	// a flag to show the reduce phase finishes
	finishReduce 		bool
	// number of reducers
	nReduce		 	int
	// number of mappers
	nMap			int
	// lock
	Mutex    		*sync.Mutex
	// a hashmap to store time info to deal with straggler for map phase
	mapStartTime	map[string]time.Time
	// a hashmap to store time info to deal with straggler for reduce phase
	reduceStartTime	map[int]time.Time
	// a ticker used to deal with straggler for map phase
	stragglerTicker *time.Ticker
	// a ticker used to deal with straggler for reduce phase
	stragglerTicker2 *time.Ticker
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// mr-main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.Mutex.Lock()
	ret = c.finishReduce
	c.Mutex.Unlock()
	return ret
}

// create a Coordinator.
// mr-main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.

	// initialize the variables of coodinator
	c.mapTrack = make(map[string]MapTask)
	c.reduceTrack = make(map[int]MapTask)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.finishMap = false 
	c.Mutex = new(sync.Mutex)
	c.mapStartTime = make(map[string]time.Time)
	c.reduceStartTime = make(map[int]time.Time)

	logger.Println("Finish initializing the coordinator.")
	// make a channel to handle task processing for map
	mapChan = make(chan MapTask, len(files))
	// make a channel to handle task processing for reduce
	reduceChan = make(chan int, c.nReduce)

	// iterate all the input files
	for index, file := range files {
		task := MapTask{}
		task.filename = file
		task.index = index
		// initialize the jobType to "map", since it is for sure that we do all the map first before doing reduce.
		task.jobType = "map"
		// for each file, initialize the status to ready to make status
		task.status = ready
		// pass the task into the map for tracking
		c.mapTrack[file] = task
		// pass the task into the channel
		mapChan <- c.mapTrack[file]
		logger.Printf("Put maptask with index: %v into map channel", task.index)
	}

	// to check if there is a straggler for map phase
	c.mapStartStragglerTicker()

	c.server()
	return &c
}

// distribute job type to wokers
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	select {
		case maptask := <-mapChan:
			// pass info into the reply
			reply.JobType = maptask.jobType
			reply.Filename = maptask.filename
			reply.Index = maptask.index
			reply.NReduce = c.nReduce
			reply.NMap = c.nMap
			logger.Printf("GetTask assigns maptask with index = %v", maptask.index)

			// store the start time for each file
			c.Mutex.Lock()
			c.mapStartTime[reply.Filename] = time.Now()
			c.Mutex.Unlock()
			logger.Printf("set start time for maptask with index = %v", maptask.index)


			// change the status of this task to started.
			maptask.status = started
			// use lock to prevent data race
			c.Mutex.Lock()
			c.mapTrack[maptask.filename] = maptask
			c.Mutex.Unlock()

			logger.Printf("maptask with index = %v from 'ready' to 'started'", maptask.index)
			return nil
		
		// if there is id going in reduceChan, this means that mappahse finishes.
		// this also means that there must be no task passing into mapChan.
		case id := <-reduceChan:
			
			// use lock to prevent data race
			c.Mutex.Lock()
			reducetask := c.reduceTrack[id]
			c.Mutex.Unlock()

			// pass info into the reply
			reply.JobType = reducetask.jobType
			reply.Index = reducetask.index
			reply.NMap = c.nMap
			
			logger.Printf("GetTask assigns id = %v", id)

			// store the start time for each id
			c.Mutex.Lock()
			c.reduceStartTime[reply.Index] = time.Now()
			c.Mutex.Unlock()
			logger.Printf("set start time for reducetask with index = %v", reducetask.index)

			// change the status of this task to started.
			reducetask.status = started

			c.Mutex.Lock()
			c.reduceTrack[id] = reducetask
			c.Mutex.Unlock()

			logger.Printf("reducetask with id = %v from 'ready' to 'started'", id)

			
			return nil
	}
}

//
func (c *Coordinator) startReduce() error {

	logger.Println("start reduce phase")

	for id := 0; id < c.nReduce; id++ {
		task := MapTask{}
		// the index here is the partition_id
		task.index = id
		// initialize the jobType to "reduce" to start reduce phase
		task.jobType = "reduce"
		// for each file, initialize the status to ready to make status
		task.status = ready
		// pass the task into the map for tracking
		c.reduceTrack[id] = task
		// pass the task into the channel
		reduceChan <- id

		logger.Printf("Put id: %v into reduce channel", task.index)
	}

	// to check if there is a straggler for reduce phase
	c.reduceStartStragglerTicker()

	return nil
}

// update status for map
func  (c *Coordinator) MapUpdate(args *MapUpStatusArgs, reply *MapUpStatusReply) error {
	// use lock to prevent data race
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// update map status
	task := c.mapTrack[args.Filename]
	task.status = end
	c.mapTrack[args.Filename] = task

	logger.Printf("update map status to end, index = %v", args.Index)

	// since the map for this file is finished, no need to hold the start time
	delete(c.mapStartTime, args.Filename)
	logger.Printf("delete map start time, index = %v", args.Index)
	
	// need a func to check if map phase ends
	c.checkMapPhase()

	logger.Println("end checkMapPhase")

	// need a func to initlize reducetrack
	if c.finishMap == true {
		c.startReduce()
	}

	return nil
}

// update status for reduce
func  (c *Coordinator) ReduceUpdate(args *ReduceUpStatusArgs, reply *ReduceUpStatusReply) error {
	// use lock to prevent data race
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	// update map status
	task := c.reduceTrack[args.Index]
	task.status = end
	c.reduceTrack[args.Index] = task

	logger.Printf("update reduce status to end, index = %v", args.Index)

	// since the reduce for this file is finished, no need to hold the start time
	delete(c.reduceStartTime, args.Index)
	logger.Printf("delete reduce start time, index = %v", args.Index)
	
	// need a func to check if reduce phase ends
	c.checkReducePhase()

	return nil
}


// func to check if all the map status are finished
func (c *Coordinator) checkReducePhase() error{
	
	logger.Println("start checkReducePhase")
	current := true
	// iterate all the status for files
	for id := range c.reduceTrack {
		reducetask := c.reduceTrack[id]
		if reducetask.status != end {
			current = false
			break
		}
	}

	// if current is true, we know that the reduce phase ends and no need to hold the ticker
	if current == true {
		c.stragglerTicker2.Stop()
		logger.Println("stop stragglerTicker for reduce")
		c.finishReduce = true
	}

	logger.Printf("finishedReduce = %v", current)
	
	return nil
}


// func to check if all the map status are finished
func (c *Coordinator) checkMapPhase() error{
	
	logger.Println("start checkMapPhase")
	current := true
	// iterate all the status for files
	for file := range c.mapTrack {
		maptask := c.mapTrack[file]
		if maptask.status != end {
			current = false
			break
		}
	}

	// if current is true, we know that the map phase ends and no need to hold the ticker
	if current == true {
		c.stragglerTicker.Stop()
		logger.Println("stop stragglerTicker for map")
		c.finishMap = true
	}

	logger.Printf("finishedMap = %v", current)
	
	return nil
}

// func to continuosly check if there is a straggler for map phase
func (c *Coordinator) mapStartStragglerTicker() {
	// Check every 5 seconds
	c.stragglerTicker = time.NewTicker(5 * time.Second) 

	// for each worker we need to check straggler
	go func() {
		for range c.stragglerTicker.C {
			c.Mutex.Lock()
			for filename, task := range c.mapTrack {
				if task.status == started {
					// 10 second is the time limit
					if time.Since(c.mapStartTime[filename]) > 10*time.Second { 
						logger.Printf("Task on file %v is a straggler and will be re-enqueued.", filename)
						// change the straggler's file into ready again
						task.status = ready
						c.mapTrack[filename] = task
						// pass the task back to the chan to assign to other worker
						mapChan <- task 
					}
				}
			}
			c.Mutex.Unlock()
		}
	}()
}

// func to continuosly check if there is a straggler for reduce phase
func (c *Coordinator) reduceStartStragglerTicker() {
	// Check every 5 seconds
	c.stragglerTicker2 = time.NewTicker(5 * time.Second) 

	// for each worker we need to check straggler
	go func() {
		for range c.stragglerTicker2.C {
			c.Mutex.Lock()
			for id, task := range c.reduceTrack {
				if task.status == started {
					// 10 second is the time limit
					if time.Since(c.reduceStartTime[id]) > 10*time.Second { 
						logger.Printf("Task on file %v is a straggler and will be re-enqueued.", id)
						// change the straggler's file into ready again
						task.status = ready
						c.reduceTrack[id] = task
						// pass the id back to the chan to assign to other worker
						reduceChan <- id
					}
				}
			}
			c.Mutex.Unlock()
		}
	}()
}


// start a thread that listens for RPCs from worker.go
// DO NOT MODIFY
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

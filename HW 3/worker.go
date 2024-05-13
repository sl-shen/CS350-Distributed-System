package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"io/ioutil"
	"os"
	"encoding/json"
	"sort"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// mr-main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// make rpc call to receive what task to do from master and let worker do the task
	for {
		reply := CallCoordinator()
		logger.Println("Get reply by CallCoordinator")
		if reply.JobType == "map" {
			doMap(mapf, reply)
		} else if reply.JobType == "reduce" {
			doReduce(reducef, reply)
		} else {
			break
		}
	}
}

// do the map task
func doMap(mapf func(string, string) []KeyValue, reply GetTaskReply) {
	logger.Printf("Start doMap, index = %v", reply.Index)
	// create a intermediate file to save each map result to correspondent partition id
	intermediate := make(map[int] []KeyValue)
	
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", reply.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.Filename)
	}
	file.Close()
	// kvs is a slice of keyvalue pairs
	kvs := mapf(reply.Filename, string(content))
		
	for _, kv := range kvs {
	// Determine the partition_id
	partitionID := ihash(kv.Key) % reply.NReduce
	// Append to intermediate
	intermediate[partitionID] = append(intermediate[partitionID], kv)
	}
	
	// Write the intermediate KeyValue pairs to their respective files
	for partitionID, kvs := range intermediate {
	// Use a filename that includes the map task number and the partition number
		filename := fmt.Sprintf("mr-%d-%d.json", reply.Index, partitionID)
		file, err := os.Create(filename)
		if err != nil {
			logger.Fatalf("cannot create file %v", filename)
		}
		
		encoder := json.NewEncoder(file)
		err = encoder.Encode(kvs)
		if err != nil {
			logger.Fatalf("cannot encode KeyValue pairs to file %v", filename)
		}
		file.Close()
		logger.Printf("doMap: wrote %d KeyValue pairs to file %v", len(kvs), filename)
	}
	
	logger.Printf("Finish doMap, index = %v", reply.Index)
	
	
	MapUpdateStatus(reply.Filename, reply.Index)

}

// do the reduce task
func doReduce(reducef func(string, []string) string, reply GetTaskReply) {
	logger.Printf("Start doReduce, index = %v", reply.Index)

	// a slice to hold all pairs
	var intermediate []KeyValue

    // Iterate over all map tasks to gather intermediate KeyValue pairs for this reduce task
    for i := 0; i < reply.NMap; i++ { 
        filename := fmt.Sprintf("mr-%d-%d.json", i, reply.Index)
        file, err := os.Open(filename)
        if err != nil {
			// need to use normal printf, fatal log will cause issue, since some pair of mr-x-x will miss, fatal log will pause the process.
            logger.Printf("Warning: cannot open %v", filename)
			continue
        }
        decoder := json.NewDecoder(file)
        var kvs []KeyValue
        err = decoder.Decode(&kvs)
        if err != nil {
			// need to use normal printf, fatal log will cause issue, since some pair of mr-x-x will miss, fatal log will pause the process.
            logger.Printf("Warning: cannot decode KeyValue pairs from file %v", filename)
			file.Close()
			continue
        }
        file.Close()
		
        // Append to intermediate slice
        intermediate = append(intermediate, kvs...)

	}	

	//for _, kv := range intermediate {
        //logger.Printf("Intermediate key-value pair: %v, %v", kv.Key, kv.Value)
    //}

	// do the sort
	sort.Sort(ByKey(intermediate))

	// Create output file
	oname := fmt.Sprintf("mr-out-%v", reply.Index)
	ofile, err := os.Create(oname)
	if err != nil {
		log.Fatalf("cannot create %v", oname)
	}

	logger.Println("call reduce func")
	// Call Reduce on each distinct key and write output to the file

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		//logger.Printf("Reduce output for key %v: %v", intermediate[i].Key, output)
	
		// Write output to file
		_, err := fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			logger.Printf("Error writing to file %v: %v", oname, err)
		}

		i = j
	}
	
	ofile.Close()
	logger.Printf("Finish doReduce, index = %v", reply.Index)


	ReduceUpdateStatus(reply.Index)

}


// make rpc call to update the status of map in coordinator
func MapUpdateStatus(filename string, index int) {
	args := MapUpStatusArgs{}
	args.Filename = filename
	args.Index = index
	reply := MapUpStatusReply{}
	call("Coordinator.MapUpdate", &args, &reply)
}

// make rpc call to update the status of reduce in coordinator
func ReduceUpdateStatus(index int) {
	args := ReduceUpStatusArgs{}
	args.Index = index
	reply := ReduceUpStatusReply{}
	call("Coordinator.ReduceUpdate", &args, &reply)
}

// make rpc call
func CallCoordinator() GetTaskReply {
	args := GetTaskArgs{}
	reply := GetTaskReply{}
	// send rpc request
	call("Coordinator.GetTask", &args, &reply)
	return reply
}



// example function to show how to make an RPC call to the coordinator.
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
// DO NOT MODIFY
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("Unable to Call", rpcname, "- Got error:", err)
	return false
}

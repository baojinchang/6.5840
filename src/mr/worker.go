package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type TaskInfo struct {
	FileName    string
	FileIndex   int
	Status      string
	ReduceIndex int
	Time        int64
}

type KeyValue struct {
	Key   string
	Value string
}

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
	arg := TaskInfo{}
	reply := TaskInfo{}
	for true {
		_ = call("Coordinator.RequestTask", &arg, &reply)
		switch reply.Status {
		case "map":
			TaskMap(mapf, &reply)
			break
		case "reduce":
			TaskReduce(reducef, &reply)
			break
		case "wait":
			time.Sleep(time.Second)
			break
		case "":
			fmt.Printf("Nothing")
			return
		}
	}
	return
}

func TaskMap(mapf func(string, string) []KeyValue, reply *TaskInfo) {
	file, err := os.Open(reply.FileName)
	if err != nil {
		log.Fatalf("cannot open %v", reply.FileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", reply.FileName)
	}
	file.Close()
	kva := mapf(reply.FileName, string(content))
	kvas := make([][]KeyValue, reply.ReduceIndex)
	for i := 0; i < reply.ReduceIndex; i++ {
		kvas[i] = make([]KeyValue, 0)
	}
	for _, item := range kva {
		index := ihash(item.Key) % reply.ReduceIndex
		kvas[index] = append(kvas[index], item)
	}
	for i := 0; i < reply.ReduceIndex; i++ {
		tempfile, err := os.CreateTemp(".", "mrtemp")
		if err != nil {
			fmt.Println("error")
		}
		enc := json.NewEncoder(tempfile)
		for _, kv := range kvas[i] {
			err := enc.Encode(&kv)
			if err != nil {
				fmt.Println("Json encode failed")
			}
		}
		outname := fmt.Sprintf("mr-%v-%v", reply.FileIndex, i)
		err = os.Rename(tempfile.Name(), outname)
		if err != nil {
			fmt.Println("error")
		}
	}
	task := TaskInfo{}
	call("Coordinator.TaskDone", &reply, &task)
	return
}

func TaskReduce(reducef func(string, []string) string, reply *TaskInfo) {
	reply.ReduceIndex -= 1
	intermediate := []KeyValue{}
	for i := 0; i < reply.FileIndex; i++ {
		fileName := "mr-"
		fileName += strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceIndex)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", reply.FileName)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))
	tempfile, err := os.CreateTemp(".", "mrtemp")
	if err != nil {
		fmt.Println("error")
	}
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

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempfile, "%v %v\n", intermediate[i].Key, output)
		i = j
	}
	outname := fmt.Sprintf("mr-out-%v", reply.ReduceIndex)
	err = os.Rename(tempfile.Name(), outname)
	if err != nil {
		fmt.Println("error")
	}
	task := TaskInfo{}
	call("Coordinator.TaskDone", &reply, &task)
	return
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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

	fmt.Println(err)
	return false
}

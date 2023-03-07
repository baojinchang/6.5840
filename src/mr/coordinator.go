package mr

import (
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	mapWait    chan TaskInfo
	mapRun     chan TaskInfo
	reduceWait chan TaskInfo
	reduceRun  chan TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) RequestTask(args *TaskInfo, reply *TaskInfo) error {
	if len(c.mapWait) != 0 {
		*reply = <-c.mapWait
		reply.Status = "map"
		reply.Time = GetNowTime()
		c.mapRun <- *reply

		return nil
	}
	if len(c.mapRun) == 0 && len(c.mapWait) == 0 && len(c.reduceWait) != 0 {
		*reply = <-c.reduceWait
		reply.Status = "reduce"
		reply.Time = GetNowTime()
		c.reduceRun <- *reply
		return nil
	}
	reply.Status = "wait"

	return nil
}

func (c *Coordinator) TaskDone(args *TaskInfo, reply *TaskInfo) error {
	if args.Status == "map" {
		for {
			select {
			case item := <-c.mapRun:
				{
					if args.FileIndex != item.FileIndex {
						c.mapRun <- item
					}
					if args.FileIndex == item.FileIndex {
						return nil
					}
				}
			}
		}
	}
	if args.Status == "reduce" {
		for {
			select {
			case item := <-c.reduceRun:
				{
					if args.ReduceIndex != item.ReduceIndex {
						c.reduceRun <- item
					}
					if args.ReduceIndex == item.ReduceIndex {
						return nil
					}
				}
			}
		}
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

func (c *Coordinator) RemoveTimeOutTask() {
	for {
		time.Sleep(time.Second)
		if len(c.mapRun) != 0 {
			select {
			case item := <-c.mapRun:
				{
					if GetNowTime()-item.Time < 10 {
						c.mapRun <- item
					} else {
						c.mapWait <- item
					}
				}
			}
		}
		if len(c.reduceRun) != 0 {
			select {
			case item := <-c.reduceRun:
				{
					if GetNowTime()-item.Time < 10 {
						c.reduceRun <- item
					} else {
						c.reduceWait <- item
					}
				}
			}
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	if len(c.mapWait) == 0 && len(c.mapRun) == 0 && len(c.reduceRun) == 0 && len(c.reduceWait) == 0 {
		return true
	}

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{make(chan TaskInfo, 10), make(chan TaskInfo, 10), make(chan TaskInfo, 10), make(chan TaskInfo, 10)}
	for i := 0; i < len(files); i++ {
		task := TaskInfo{files[i], i, "map", nReduce, 0}
		c.mapWait <- task
	}
	for i := 0; i < nReduce; i++ {
		reduce := TaskInfo{"aaa", len(files), "reduce", i + 1, 0}
		c.reduceWait <- reduce
	}
	go c.RemoveTimeOutTask()
	c.server()
	return &c
}

func GetNowTime() int64 {
	return time.Now().UnixNano() / int64(time.Second)
}

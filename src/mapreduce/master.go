package mapreduce

//
// Please do not modify this file.
//

import (
	"fmt"
	"net"
	"sync"
)

// Master 拥有 Master 需要跟踪的所有状态。
type Master struct {
	sync.Mutex

	address     string
	doneChannel chan bool

	// 受互斥体保护
	newCond *sync.Cond // 当 Register() 添加到 workers[] 时发出信号
	workers []string   // each worker's UNIX 域套接字名称——它的 RPC 地址

	// 每个任务的信息
	jobName string   // 当前执行作业的名称
	files   []string // Input files
	nReduce int      // Reduce分区数

	shutdown chan struct{}
	l        net.Listener
	stats    []int
}

// Register 是一个 RPC 方法，worker 启动后调用它来报告他们已准备好接收任务。
func (mr *Master) Register(args *RegisterArgs, _ *struct{}) error {
	mr.Lock()
	defer mr.Unlock()
	debug("Register: worker %s\n", args.Worker)
	mr.workers = append(mr.workers, args.Worker)

	// 告诉 forwardRegistrations() 有一个新的 workers[] 条目。
	mr.newCond.Broadcast()

	return nil
}

// newMaster 初始化一个新的 MapReduce Master
func newMaster(master string) (mr *Master) {
	mr = new(Master)
	mr.address = master
	mr.shutdown = make(chan struct{})
	mr.newCond = sync.NewCond(mr)
	mr.doneChannel = make(chan bool)
	return
}

// Sequential 依次运行 map 和 reduce 任务，等待每个任务完成后再运行下一个任务。
func Sequential(jobName string, files []string, nreduce int,
	mapF func(string, string) []KeyValue,
	reduceF func(string, []string) string,
) (mr *Master) {
	mr = newMaster("master")
	go mr.run(jobName, files, nreduce, func(phase jobPhase) {
		switch phase {
		case mapPhase:
			for i, f := range mr.files {
				doMap(mr.jobName, i, f, mr.nReduce, mapF)
			}
		case reducePhase:
			for i := 0; i < mr.nReduce; i++ {
				doReduce(mr.jobName, i, mergeName(mr.jobName, i), len(mr.files), reduceF)
			}
		}
	}, func() {
		mr.stats = []int{len(files) + nreduce}
	})
	return
}

// helper function that sends information about all existing
// and newly registered workers to channel ch. schedule()
// reads ch to learn about workers.
func (mr *Master) forwardRegistrations(ch chan string) {
	i := 0
	for {
		mr.Lock()
		if len(mr.workers) > i {
			// there's a worker that we haven't told schedule() about.
			w := mr.workers[i]
			go func() { ch <- w }() // send without holding the lock.
			i = i + 1
		} else {
			// wait for Register() to add an entry to workers[]
			// in response to an RPC from a new worker.
			mr.newCond.Wait()
		}
		mr.Unlock()
	}
}

// Distributed 分布式调度在通过 RPC 向主服务器注册的工作人员上映射和减少任务。
func Distributed(jobName string, files []string, nreduce int, master string) (mr *Master) {
	mr = newMaster(master)
	mr.startRPCServer()
	go mr.run(jobName, files, nreduce,
		func(phase jobPhase) {
			ch := make(chan string)
			go mr.forwardRegistrations(ch)
			schedule(mr.jobName, mr.files, mr.nReduce, phase, ch)
		},
		func() {
			mr.stats = mr.killWorkers()
			mr.stopRPCServer()
		})
	return
}

// run executes a mapreduce job on the given number of mappers and reducers.
//
// First, it divides up the input file among the given number of mappers, and
// schedules each task on workers as they become available. Each map task bins
// its output in a number of bins equal to the given number of reduce tasks.
// Once all the mappers have finished, workers are assigned reduce tasks.
//
// When all tasks have been completed, the reducer outputs are merged,
// statistics are collected, and the master is shut down.
//
// Note that this implementation assumes a shared file system.
func (mr *Master) run(jobName string, files []string, nreduce int,
	schedule func(phase jobPhase),
	finish func(),
) {
	mr.jobName = jobName
	mr.files = files
	mr.nReduce = nreduce

	fmt.Printf("%s: Starting Map/Reduce task %s\n", mr.address, mr.jobName)

	schedule(mapPhase)
	schedule(reducePhase)
	finish()
	mr.merge()

	fmt.Printf("%s: Map/Reduce task completed\n", mr.address)

	mr.doneChannel <- true
}

// Wait blocks until the currently scheduled work has completed.
// This happens when all tasks have scheduled and completed, the final output
// have been computed, and all workers have been shut down.
func (mr *Master) Wait() {
	<-mr.doneChannel
}

// killWorkers cleans up all workers by sending each one a Shutdown RPC.
// It also collects and returns the number of tasks each worker has performed.
func (mr *Master) killWorkers() []int {
	mr.Lock()
	defer mr.Unlock()
	ntasks := make([]int, 0, len(mr.workers))
	for _, w := range mr.workers {
		debug("Master: shutdown worker %s\n", w)
		var reply ShutdownReply
		ok := call(w, "Worker.Shutdown", new(struct{}), &reply)
		if ok == false {
			fmt.Printf("Master: RPC %s shutdown error\n", w)
		} else {
			ntasks = append(ntasks, reply.Ntasks)
		}
	}
	return ntasks
}

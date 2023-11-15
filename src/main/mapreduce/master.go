package mapreduce

import "container/list"
import "fmt"

type WorkerInfo struct {
	address string
	// You can add definitions here.
}

func assignJobsToWorkers(mr *MapReduce, doneChannel chan int, job JobType, nJobs int, nOtherJobs int) {
	for i := 0; i < nJobs; i++ {
		go func(jobNum int) {
			worker := <-mr.registerChannel

			args := &DoJobArgs{mr.file, job, jobNum, nOtherJobs}
			var reply DoJobReply

			ok := call(worker, "Worker.DoJob", args, &reply)
			if ok == true {
				doneChannel <- 1
				mr.registerChannel <- worker
			}

		}(i)
	}
}

// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {
	// Your code here
	mapDoneChannel := make(chan int, mr.nMap)
	reduceDoneChannel := make(chan int, mr.nReduce)

	// map phase
	assignJobsToWorkers(mr, mapDoneChannel, Map, mr.nMap, mr.nReduce)

	// await map phase completion
	for i := 0; i < mr.nMap; i++ {
		<-mapDoneChannel
	}

	// reduce phase
	assignJobsToWorkers(mr, reduceDoneChannel, Reduce, mr.nReduce, mr.nMap)

	// await reduce phase completion
	for i := 0; i < mr.nReduce; i++ {
		<-reduceDoneChannel
	}

	return mr.KillWorkers()
}

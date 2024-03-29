package mapreduce

import (
	"fmt"
	"sync"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)

	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce   // number of intermediate files
		//fmt.Println("mapPhase")
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
		//fmt.Println("mapPhase")
	}

	var  wait_group sync.WaitGroup

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	for i := 0; i < ntasks; i++ {
		wait_group.Add(1)
		go func(task int){
			for true {
				current_worker := <- registerChan

				args := DoTaskArgs{
					JobName: jobName,
					File: mapFiles[task],
					Phase: phase,
					TaskNumber: task,
					NumOtherPhase: n_other,
				}

				if call(current_worker, "Worker.DoTask", args, new(struct{})){
					wait_group.Done()  // Done() = Add(-1)
					registerChan <- current_worker
					break;
				}
			}
		}(i)
	}

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part 2, 2B).
	//
	wait_group.Wait()
	fmt.Printf("Schedule: %v done\n", phase)
}

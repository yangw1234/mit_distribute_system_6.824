package mapreduce

import (
	"fmt"
)

type Task struct {
	Worker string
	TaskNum int
}

// schedule starts and waits for all tasks in the given phase (Map or Reduce).
func (mr *Master) schedule(phase jobPhase) {
	var ntasks int
	var nios int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mr.files)
		nios = mr.nReduce
	case reducePhase:
		ntasks = mr.nReduce
		nios = len(mr.files)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, nios)

	runningWorkers := make(map[string]bool)

	for _, worker := range mr.workers {
		runningWorkers[worker] = false
	}

	doneChan := make(chan Task)
	failChan := make(chan Task)
	doneTasks := 0
	runningTasks := make(map[int]bool)
	remainingTasks := make(map[int]bool)
	for i := 0; i < ntasks; i++ {
		remainingTasks[i] = true
	}

	for doneTasks < ntasks {
		select {
		case worker := <- mr.registerChannel:
			_, ok := runningWorkers[worker]
			if !ok {
				runningWorkers[worker] = false
			}
		case doneTask := <- doneChan:
			runningWorkers[doneTask.Worker] = false
		        delete(runningTasks, doneTask.TaskNum)
			doneTasks ++
		case failTask := <- failChan:
			delete(runningWorkers, failTask.Worker)
			delete(runningTasks, failTask.TaskNum)
			remainingTasks[failTask.TaskNum] = true

		default:
			if len(remainingTasks) > 0 {
				for worker, running := range runningWorkers {
					if !running {
						task := 0
						for t, _ := range remainingTasks {
							task = t
							break
						}
						args := DoTaskArgs{
							JobName: mr.jobName,
							File:mr.files[task],
							Phase: phase,
							TaskNumber: task,
							NumOtherPhase: nios,
						}
						delete(remainingTasks, task)
						runningWorkers[worker] = true
						runningTasks[task] = true
						go func() {
							success := call(worker, "Worker.DoTask", args, nil)
							if success {
								doneChan <- Task{worker, task}
							} else {
								failChan <- Task{worker, task}
							}
						}()
						break
					}
				}
			}
		}
	}

	// All ntasks tasks have to be scheduled on workers, and only once all of
	// them have been completed successfully should the function return.
	// Remember that workers may fail, and that any given worker may finish
	// multiple tasks.
	//
	// TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO TODO
	//
	fmt.Printf("Schedule: %v phase done\n", phase)
}

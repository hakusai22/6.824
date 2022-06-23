package mapreduce

import (
	"fmt"
	"strconv"
)

// Debugging 开启
const debugEnabled = false

// debug() 仅在 debugEnabled 为 true 时打印
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// jobPhase 指示任务是被调度为 map 任务还是 reduce 任务。
type jobPhase string

const (
	mapPhase    jobPhase = "mapPhase"
	reducePhase          = "reducePhase"
)

// KeyValue 是一种用于保存传递给 map 和 reduce 函数的键值对的类型。
type KeyValue struct {
	Key   string
	Value string
}

// reduceName 构造映射任务的中间文件的名称
// <mapTask> 为reduce 任务<reduceTask> 生成。
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mapreduce." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName 构造reduce任务<reduceTask>的输出文件名
func mergeName(jobName string, reduceTask int) string {
	return "mapreduce." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

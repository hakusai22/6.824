package main

import (
	"fmt"
	"mapreduce"
	"os"
	"strconv"
	"strings"
	"unicode"
)

// 每个输入文件都会调用一次 map 函数。第一个参数是输入文件的名称，第二个参数是文件的完整内容。
//忽略输入文件名，只查看内容参数。返回值是键值对的一部分。
func mapFunction(filename string, contents string) []mapreduce.KeyValue {
	// Your code here (Part II).
	// FieldsFunc 以一个或多个满足 f(rune) 的字符为分隔符，
	// 将 s 切分成多个子串，结果中不包含分隔符本身。
	// 如果 s 中没有满足 f(rune) 的字符，则返回一个空列表。
	words := strings.FieldsFunc(contents, func(r rune) bool {
		return !unicode.IsLetter(r)
	})
	//申请数组空间
	kv := make([]mapreduce.KeyValue, 0)
	for _, word := range words {
		kv = append(kv, mapreduce.KeyValue{word, "1"})
	}
	return kv
}

// 对于 map 任务生成的每个键，reduce 函数都会调用一次，其中包含任何 map 任务为该键创建的所有值的列表。
func reduceFunction(key string, values []string) string {
	// Your code here (Part II).
	//strconv.Itoa函数的参数是一个整型数字，它可以将数字转换成对应的字符串类型的数字。
	return strconv.Itoa(len(values))
}

// 可以通过3种方式运行：
// 1) Sequential (e.g., go run wc.go master sequential x1.txt .. xN.txt)
// 2) Master (e.g., go run wc.go master localhost:7777 x1.txt .. xN.txt)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
	if len(os.Args) < 4 {
		fmt.Printf("%s: see usage comments in file\n", os.Args[0])
		//master
	} else if os.Args[1] == "master" {
		var mr *mapreduce.Master
		if os.Args[2] == "sequential" {
			mr = mapreduce.Sequential("wcSequential", os.Args[3:], 3, mapFunction, reduceFunction)
		} else {
			mr = mapreduce.Distributed("wcDistributed", os.Args[3:], 3, os.Args[2])
		}
		mr.Wait()
		// worker
	} else {
		mapreduce.RunWorker(os.Args[2], os.Args[3], mapFunction, reduceFunction, 100, nil)
	}
}

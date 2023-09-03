package raft

import "log"

// Debugging
const Debug = true

func DPrint(a ...interface{}) (n int, err error) {
	if Debug {
		log.Print(a...)
	}
	return
}

package raft

import "log"

// Debugging
const Debug = 1

func Pf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

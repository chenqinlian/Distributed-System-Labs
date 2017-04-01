package raft

import "log"

// Debugging
const Debug = 1

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func Assert(statement bool, format string, a ...interface{}) {
    if !statement {
        DPrintf(format, a...)
        panic("Assertion Failed")
    }
}

func (rf *Raft) checkNextIdx(idx int) {
    if rf.nextIdx[idx] > len(rf.Log) {
        DPrintf("%s: rf.nextIdx[%d] = %d, len(rf.Log) = %d", rf, idx, rf.nextIdx[idx], len(rf.Log))
    }
}

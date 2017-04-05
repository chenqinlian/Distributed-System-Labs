package raft

import "log"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

func assert(statement bool, format string, a ...interface{}) {
    if !statement {
        DPrintf(format, a...)
        panic("Assertion Failed")
    }
}

func (rf *Raft) checkNextIdx(idx int) {
    assert(rf.nextIdx[idx] > rf.BaseIdx && rf.nextIdx[idx] <= rf.BaseIdx+len(rf.Log),
        "%s: rf.nextIdx[%d] = %d, rf.BaseIdx = %d, len(rf.Log) = %d",
        rf, idx, rf.nextIdx[idx], rf.BaseIdx, len(rf.Log))
}

func min(firstItem int, items ...int) (result int) {
    result = firstItem
    for _, item := range items {
        if item < result {
            result = item
        }
    }
    return
}

func max(firstItem int, items ...int) (result int) {
    result = firstItem
    for _, item := range items {
        if item > result {
            result = item
        }
    }
    return
}

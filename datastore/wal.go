package datastore

import "sync"

const (
	walInactive int = iota
	walWriting
	walDraining
)

type walStatus struct {
	sync.RWMutex
	state int // one of the wal* constants
}

type ipWAL struct {
	sync.RWMutex
	m      IPDataMap
	status walStatus
}

func (wal *ipWAL) getMap() IPDataMap {
	return wal.m
}

func newIPWAL() *ipWAL {
	wal := new(ipWAL)
	wal.m = make(IPDataMap)
	return wal
}

func (wal *ipWAL) getIPs() []IPLong {
	wal.RLock()
	defer wal.RUnlock()

	var keys []IPLong
	for k := range wal.m {
		keys = append(keys, k)
	}
	return keys
}

package datastore

import (
	"log"
	"os"
	"path/filepath"
	"sync"
)

const kdbFile = "kawana.kdb"

type BWModifier int

const (
	BWNop BWModifier = iota
	BWWhitelist
	BWUnWhitelist
	BWBlacklist
	BWUnBlacklist
)

// IPDataStore is a lockable struct which holds data about IPs,
// a write-ahead log, and options
type IPDataStore struct {
	sync.RWMutex
	s3Bucket string
	dataDir  string
	m        IPDataMap
	wal      *ipWAL
}

type syncIPDataStore interface {
	sync.Locker
	RLock()
	RUnlock()
	getMap() IPDataMap
}

func (store *IPDataStore) getMap() IPDataMap {
	return store.m
}

// New creates a new IPDataStore
func New(dataDir, s3Bucket string) *IPDataStore {
	s, err := newFromFile(dataDir, s3Bucket)
	if err == nil {
		// kdb file existed and loaded successfully
		return s
	}

	if !os.IsNotExist(err) {
		//kdb failed to load
		log.Fatal(kdbFile + " appears to be corrupted: " + err.Error())
	}

	s = new(IPDataStore)
	s.dataDir = dataDir
	s.s3Bucket = s3Bucket

	err = s.ensureDataDirExists()
	if err != nil {
		log.Fatal(err)
	}

	s.m = make(IPDataMap)
	s.wal = newIPWAL()
	return s
}

func newFromFile(dataDir, s3Bucket string) (*IPDataStore, error) {
	filename := dataDir + string(filepath.Separator) + kdbFile
	file, err := os.Open(filename)
	if err != nil {
		return new(IPDataStore), err
	}

	log.Println("Loading " + kdbFile + "...")
	dec := newDecoder(file)
	m := make(IPDataMap)
	err = dec.decode(&m)
	if err != nil {
		return new(IPDataStore), err
	}
	log.Println("Done loading")
	newStore := &IPDataStore{m: m, dataDir: dataDir, s3Bucket: s3Bucket, wal: newIPWAL()}
	return newStore, nil
}

func (store *IPDataStore) ensureDataDirExists() error {
	return os.MkdirAll(store.dataDir, 0755)
}

// LogIP adds the impact to the specified IP's time windows
// and modifies the BlackWhite list field
// Always takes a read lock on the store, takes a write lock if the IP did not exist yet
func (store *IPDataStore) LogIP(ip IPLong, impact ImpactAmount, blackWhite BWModifier) IPData {
	store.wal.status.RLock()
	defer store.wal.status.RUnlock()

	var ipStore syncIPDataStore
	state := store.wal.status.state

	if state == walWriting {
		// copy from store to wal first,
		// later do normal update
		copyIPData(store.wal, store, ip)
		ipStore = store.wal
	} else if state == walDraining {
		// try update WAL
		// if not exists in wal, will do the normal update on the store
		ipData, exists := ipStoreUpdate(store.wal, ip, impact, blackWhite)
		if !exists {
			ipStore = store
		} else {
			return ipData
		}
	} else {
		// wal inactive
		ipStore = store
	}

	ipData, exists := ipStoreUpdate(ipStore, ip, impact, blackWhite)
	if !exists {
		return ipStoreInsert(ipStore, ip, impact, blackWhite)
	}
	return ipData
}

// ForgiveIP subtracts the impacts from the specified IP's time windows
// Always takes a read lock on the store, takes a write lock on the IPData
func (store *IPDataStore) ForgiveIP(ip IPLong, impacts ImpactAmounts) IPData {
	store.wal.status.RLock()
	defer store.wal.status.RUnlock()

	var ipStore syncIPDataStore
	state := store.wal.status.state

	if state == walWriting {
		// copy from store to wal first,
		// later do normal update
		copyIPData(store.wal, store, ip)
		ipStore = store.wal
	} else if state == walDraining {
		// try update WAL
		// if not exists in wal, will operate on store
		ipData, exists := ipStoreForgive(store.wal, ip, impacts)
		if !exists {
			ipStore = store
		} else {
			return ipData
		}
	} else {
		// wal inactive
		ipStore = store
	}

	ipData, _ := ipStoreForgive(ipStore, ip, impacts)
	return ipData
}

func ipStoreForgive(store syncIPDataStore, ip IPLong, impacts ImpactAmounts) (ipData IPData, exists bool) {
	store.RLock()
	defer store.RUnlock()

	data, ok := store.getMap()[ip]
	if !ok {
		return IPData{}, false
	}

	data.forgive(impacts)
	return *data, true
}

// insertIP attempts to insert the IPData into the store.
// If the IP does not yet exist in the map, it creates the datastructure and adds it.
// If the IP already exists, it updates the existing record.
//
// Takes a write lock on the whole datastore
func ipStoreInsert(store syncIPDataStore, ip IPLong, impact ImpactAmount, blackWhite BWModifier) IPData {
	store.Lock()
	defer store.Unlock()

	var data *IPData

	data, ok := store.getMap()[ip]
	if !ok {
		data = new(IPData)
	}

	data.impact(impact, blackWhite)
	store.getMap()[ip] = data
	return *data
}

// updateIP attempts to retrieve the specified IP's data from the store.
// If the IP does not exist, it returns a zero IPData and false for existence.
// If the IP does exist, it updates the record in place.
//
// Takes a read lock on the datastore
func ipStoreUpdate(store syncIPDataStore, ip IPLong, impact ImpactAmount, blackWhite BWModifier) (IPData, bool) {
	store.RLock()
	defer store.RUnlock()

	data, ok := store.getMap()[ip]
	if !ok {
		return IPData{}, false
	}

	data.impact(impact, blackWhite)

	return *data, ok
}

func copyIPData(dst, src syncIPDataStore, ip IPLong) {
	dst.Lock()
	defer dst.Unlock()
	src.RLock()
	defer src.RUnlock()

	_, exists := dst.getMap()[ip]
	if exists {
		return
	}

	srcIPData, exists := src.getMap()[ip]
	if !exists {
		return
	}

	dst.getMap()[ip] = new(IPData)

	*dst.getMap()[ip] = *srcIPData
}

func moveIPData(dst, src syncIPDataStore, ip IPLong) {
	src.Lock()
	defer src.Unlock()
	dst.Lock()
	defer dst.Unlock()

	dst.getMap()[ip] = src.getMap()[ip]
	delete(src.getMap(), ip)
}

// Persist asynchronously saves the data store to disk
func (store *IPDataStore) Persist() error {
	store.setWALStatus(walWriting)
	err := store.writeToFile()
	store.setWALStatus(walDraining)
	store.drainWAL()
	store.setWALStatus(walInactive)

	return err
}

func (store *IPDataStore) drainWAL() {
	ips := store.wal.getIPs()
	for _, ip := range ips {
		moveIPData(store, store.wal, ip)
	}
}

func (store *IPDataStore) setWALStatus(state int) {
	store.wal.status.Lock()
	defer store.wal.status.Unlock()

	store.wal.status.state = state
}

func (store *IPDataStore) writeToFile() error {
	tmpFilename := store.kdbPath() + ".part"
	file, err := os.Create(tmpFilename)
	if err != nil {
		return err
	}
	defer file.Close()

	// write the encoded IPDataMap to temp file and fsync
	enc := newEncoder(file)
	err = enc.encode(store)
	if err != nil {
		return err
	}
	err = file.Sync()
	if err != nil {
		return err
	}

	// overwrite any old kdb with the completed new kdb
	finalFilename := store.kdbPath()
	return os.Rename(tmpFilename, finalFilename)
}

func (store *IPDataStore) kdbPath() string {
	return store.dataDir + string(filepath.Separator) + kdbFile
}

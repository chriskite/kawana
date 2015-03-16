package datastore

import (
	"encoding/binary"
	"errors"
	"io"
	"log"
	"math"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const kdbFile = "kawana.kdb"

const encodingVersion uint32 = 1

const (
	walInactive int = iota
	walWriting
	walDraining
)

type BWModifier int

const (
	BWNop BWModifier = iota
	BWWhitelist
	BWUnWhitelist
	BWBlacklist
	BWUnBlacklist
)

// IPLong is an ipv4 address as a uint32
type IPLong uint32

// ImpactAmount is an integer representing the impact of an IP addr
// over a particular time window
type ImpactAmount uint32

// ForgivenNum is an integer number of times an IP addr has been forgiven
type ForgivenNum uint16

// ImpactAmounts is a struct of 3 time windows' impacts
type ImpactAmounts struct {
	FiveMin, Hour, Day ImpactAmount
}

// StartTimes is a struct of 3 time window start times
type StartTimes struct {
	FiveMin, Hour, Day uint32
}

// IPData holds impact amounts, time window starts, forgiveness,
// and white/black list info for an IP address
type IPData struct {
	Mutex      sync.RWMutex
	CurImpacts ImpactAmounts
	MaxImpacts ImpactAmounts
	StartTimes StartTimes
	Forgiven   ForgivenNum
	BlackWhite byte
}

// IPDataMap is a map from IPLong to *IPData
type IPDataMap map[IPLong]*IPData

type walStatus struct {
	sync.RWMutex
	state int // one of the wal* constants
}

type ipWAL struct {
	sync.RWMutex
	m      IPDataMap
	status walStatus
}

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

type ipDataStoreEncoder struct {
	w io.Writer
}

type ipDataStoreDecoder struct {
	r io.Reader
}

func (store *IPDataStore) getMap() IPDataMap {
	return store.m
}

func (wal *ipWAL) getMap() IPDataMap {
	return wal.m
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

func newIPWAL() *ipWAL {
	wal := new(ipWAL)
	wal.m = make(IPDataMap)
	return wal
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

	impactIPData(data, impact, blackWhite)
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

	impactIPData(data, impact, blackWhite)

	return *data, ok
}

func blackWhiteIPData(data *IPData, blackWhite BWModifier) error {
	switch blackWhite {
	case BWWhitelist:
		data.BlackWhite |= byte(0x01)
		break
	case BWUnWhitelist:
		data.BlackWhite &= byte(0xFE)
		break
	case BWBlacklist:
		data.BlackWhite |= byte(0x02)
		break
	case BWUnBlacklist:
		data.BlackWhite &= byte(0xFD)
		break
	default:
		return errors.New("Unknown BlackWhite modifier")
	}

	return nil
}

// impactIPData updates the IPData arg in place by adding the impact to the time windows.
//
// Takes a write lock on the IPData
func impactIPData(data *IPData, impact ImpactAmount, blackWhite BWModifier) {
	data.Mutex.Lock()
	defer data.Mutex.Unlock()

	if blackWhite != BWNop {
		err := blackWhiteIPData(data, blackWhite)
		if err != nil {
			log.Println(err)
		}
	}

	if impact == 0 {
		return
	}

	// five minute window
	if time.Now().After(time.Unix(int64(data.StartTimes.FiveMin), 0).Add(5 * time.Minute)) {
		data.StartTimes.FiveMin = uint32(time.Now().Unix())
		data.CurImpacts.FiveMin = impact
	} else {
		data.CurImpacts.FiveMin = data.CurImpacts.FiveMin.add(impact)
	}
	data.MaxImpacts.FiveMin = max(data.CurImpacts.FiveMin, data.MaxImpacts.FiveMin)

	// 1 hour window
	if time.Now().After(time.Unix(int64(data.StartTimes.Hour), 0).Add(time.Hour)) {
		data.StartTimes.Hour = uint32(time.Now().Unix())
		data.CurImpacts.Hour = impact
	} else {
		data.CurImpacts.Hour = data.CurImpacts.Hour.add(impact)
	}
	data.MaxImpacts.Hour = max(data.CurImpacts.Hour, data.MaxImpacts.Hour)

	// 1 day window
	if time.Now().After(time.Unix(int64(data.StartTimes.Day), 0).Add(24 * time.Hour)) {
		data.StartTimes.Day = uint32(time.Now().Unix())
		data.CurImpacts.Day = impact
	} else {
		data.CurImpacts.Day = data.CurImpacts.Day.add(impact)
	}
	data.MaxImpacts.Day = max(data.CurImpacts.Day, data.MaxImpacts.Day)
}

// forgive subtracts the given amounts from all the IPData's impact amounts
func (data *IPData) forgive(impacts ImpactAmounts) {
	data.MaxImpacts.FiveMin = data.MaxImpacts.FiveMin.sub(impacts.FiveMin)
	data.CurImpacts.FiveMin = data.MaxImpacts.FiveMin

	data.MaxImpacts.Hour = data.MaxImpacts.Hour.sub(impacts.Hour)
	data.CurImpacts.Hour = data.MaxImpacts.Hour

	data.MaxImpacts.Day = data.MaxImpacts.Day.sub(impacts.Day)
	data.CurImpacts.Day = data.MaxImpacts.Day

	data.Forgiven++
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

func moveIPData(dst, src syncIPDataStore, ip IPLong) {
	src.Lock()
	defer src.Unlock()
	dst.Lock()
	defer dst.Unlock()

	dst.getMap()[ip] = src.getMap()[ip]
	delete(src.getMap(), ip)
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

func newEncoder(w io.Writer) *ipDataStoreEncoder {
	return &ipDataStoreEncoder{w: w}
}

func newDecoder(r io.Reader) *ipDataStoreDecoder {
	return &ipDataStoreDecoder{r: r}
}

func (enc *ipDataStoreEncoder) encode(store *IPDataStore) error {
	store.RLock()
	defer store.RUnlock()

	var buf [43]byte

	// write encoding version
	binary.LittleEndian.PutUint32(buf[0:4], uint32(encodingVersion))
	_, err := enc.w.Write(buf[0:4])
	if err != nil {
		return err
	}

	for ip, ipData := range store.m {
		// pack the ipData's individual data into a byte array
		binary.LittleEndian.PutUint32(buf[0:4], uint32(ip))
		binary.LittleEndian.PutUint32(buf[4:8], uint32(ipData.CurImpacts.FiveMin))
		binary.LittleEndian.PutUint32(buf[8:12], uint32(ipData.CurImpacts.Hour))
		binary.LittleEndian.PutUint32(buf[12:16], uint32(ipData.CurImpacts.Day))
		binary.LittleEndian.PutUint32(buf[16:20], uint32(ipData.MaxImpacts.FiveMin))
		binary.LittleEndian.PutUint32(buf[20:24], uint32(ipData.MaxImpacts.Hour))
		binary.LittleEndian.PutUint32(buf[24:28], uint32(ipData.MaxImpacts.Day))
		binary.LittleEndian.PutUint32(buf[28:32], uint32(ipData.StartTimes.FiveMin))
		binary.LittleEndian.PutUint32(buf[32:36], uint32(ipData.StartTimes.Hour))
		binary.LittleEndian.PutUint32(buf[36:40], uint32(ipData.StartTimes.Day))
		binary.LittleEndian.PutUint16(buf[40:42], uint16(ipData.Forgiven))
		buf[42] = ipData.BlackWhite

		// write the buffer
		_, err := enc.w.Write(buf[0:])
		if err != nil {
			return err
		}
	}

	return nil
}

func (dec *ipDataStoreDecoder) decode(m *IPDataMap) error {
	// read encoding version
	var buf [43]byte
	var version uint32

	_, err := dec.r.Read(buf[0:4])
	if err != nil {
		return err
	}
	version = binary.LittleEndian.Uint32(buf[0:4])
	if version != encodingVersion {
		return errors.New("Wrong version kdb")
	}

FileLoop:
	for {
		var ip IPLong
		ipData := new(IPData)

		for num := 0; num < 43; {
			n, err := dec.r.Read(buf[0:])
			if err == io.EOF {
				break FileLoop
			} else if err != nil {
				return err
			}
			num += n
		}

		// unpack buf into ip and the fields of IPData
		ip = IPLong(binary.LittleEndian.Uint32(buf[0:4]))
		ipData.CurImpacts.FiveMin = ImpactAmount(binary.LittleEndian.Uint32(buf[4:8]))
		ipData.CurImpacts.Hour = ImpactAmount(binary.LittleEndian.Uint32(buf[8:12]))
		ipData.CurImpacts.Day = ImpactAmount(binary.LittleEndian.Uint32(buf[12:16]))
		ipData.MaxImpacts.FiveMin = ImpactAmount(binary.LittleEndian.Uint32(buf[16:20]))
		ipData.MaxImpacts.Hour = ImpactAmount(binary.LittleEndian.Uint32(buf[20:24]))
		ipData.MaxImpacts.Day = ImpactAmount(binary.LittleEndian.Uint32(buf[24:28]))
		ipData.StartTimes.FiveMin = binary.LittleEndian.Uint32(buf[28:32])
		ipData.StartTimes.Hour = binary.LittleEndian.Uint32(buf[32:36])
		ipData.StartTimes.Day = binary.LittleEndian.Uint32(buf[36:40])
		ipData.Forgiven = ForgivenNum(binary.LittleEndian.Uint16(buf[40:42]))
		buf[42] = ipData.BlackWhite

		(*m)[ip] = ipData
	}

	return nil
}

// add adds 2 impact amounts and returns the result.
// If the result would be larger than a uint32, it instead returns MaxUint32
func (a ImpactAmount) add(b ImpactAmount) ImpactAmount {
	sum := uint64(a) + uint64(b)
	if sum > uint64(math.MaxUint32) {
		return math.MaxUint32
	}
	return ImpactAmount(sum)
}

// sub subtracts 2 impact amounts and returns the result.
// If the result would be negative, it instead returns 0
func (a ImpactAmount) sub(b ImpactAmount) ImpactAmount {
	if b >= a {
		return 0
	}
	return a - b
}

// max returns the larger of 2 impact amounts
func max(a, b ImpactAmount) ImpactAmount {
	if a > b {
		return a
	}
	return b
}

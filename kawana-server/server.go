package main

import (
	"encoding/binary"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/chriskite/kawana/datastore"
)

const tcpTimeout = 5 // seconds

const (
	cmdLogIP        = 0x01
	cmdForgiveIP    = 0x02
	cmdBlackWhiteIP = 0x03
)

// Server is a Kawana TCP server that accepts commands
// via socket connections, and processes the IP data
type Server struct {
	port            int
	persistInterval time.Duration
	backupInterval  time.Duration
	s3Bucket        string
	store           *datastore.IPDataStore
	stats           stats
}

type stats struct {
	cmdsThisSec uint64
}

type command uint8

var cmdsPerSec = expvar.NewInt("cmdsPerSec")

// New creates a new Kawana Server
func New(port int, dataDir string, persistInterval int, backupInterval int, s3Bucket string) *Server {
	s := new(Server)
	s.port = port
	s.persistInterval = time.Duration(persistInterval) * time.Second
	s.backupInterval = time.Duration(backupInterval) * time.Second
	s.s3Bucket = s3Bucket

	s.store = datastore.New(dataDir, s3Bucket)
	return s
}

// Start begins accepting connections and loops forever. Also
// saves the data store to disk periodically
func (server *Server) Start() {
	server.persistEvery(server.persistInterval)
	server.backupEvery(server.backupInterval)

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", server.port))
	if err != nil {
		log.Fatal(err)
	}

	go server.startExpVar()

	// stats collector
	go func() {
		tc := time.NewTicker(time.Duration(1) * time.Second).C
		for {
			<-tc
			cmdsPerSec.Set(int64(server.stats.cmdsThisSec))
			server.stats.cmdsThisSec = 0
		}
	}()

	log.Println("Server started")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println(err)
			continue
		}
		go server.handleConnection(conn)
	}
}

func (server *Server) startExpVar() {
	http.ListenAndServe(":9292", nil)
}

func (server *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	conn.SetReadDeadline(time.Now().Add(tcpTimeout * time.Second))

	var buf [1]byte

	// read the first byte which contains the command
	_, err := io.ReadFull(conn, buf[0:])
	if err != nil {
		return
	}

	err = server.handleCommand(command(buf[0]), conn)
	if err != nil {
		log.Println(err)
		return
	}
}

func (server *Server) handleCommand(cmd command, conn io.ReadWriter) error {
	atomic.AddUint64(&server.stats.cmdsThisSec, 1)
	switch cmd {
	case cmdLogIP:
		return server.handleLogIP(conn)
	case cmdForgiveIP:
		return server.handleForgiveIP(conn)
	case cmdBlackWhiteIP:
		return server.handleBlackWhiteIP(conn)
	default:
		return errors.New("Unknown command")
	}
}

func (server *Server) handleBlackWhiteIP(conn io.ReadWriter) error {
	// BW command data is:
	// [4 byte little endian IP][1 byte bw modifier]
	var buf [5]byte
	_, err := io.ReadFull(conn, buf[0:])
	if err != nil {
		return err
	}

	ip := binary.LittleEndian.Uint32(buf[0:4])
	bwMod := buf[4] // blackwhite modifier. see datastore.BW*

	ipData := server.store.LogIP(datastore.IPLong(ip), datastore.ImpactAmount(0), datastore.BWModifier(bwMod))

	return writeIPData(ipData, conn)
}

func (server *Server) handleLogIP(conn io.ReadWriter) error {
	// LogIP command data is:
	// [4 byte little endian IP][4 byte little endian impact]
	var buf [8]byte
	_, err := io.ReadFull(conn, buf[0:])
	if err != nil {
		return err
	}

	ip := binary.LittleEndian.Uint32(buf[0:4])
	impact := binary.LittleEndian.Uint32(buf[4:8])

	ipData := server.store.LogIP(datastore.IPLong(ip), datastore.ImpactAmount(impact), datastore.BWNop)

	return writeIPData(ipData, conn)
}

func (server *Server) handleForgiveIP(conn io.ReadWriter) error {
	// ForgiveIP command data is:
	// [4 byte little endian IP][4 byte little endian 5m impact][4 byte LE hour impact][4 byte LE day impact]
	var buf [16]byte
	_, err := io.ReadFull(conn, buf[0:])
	if err != nil {
		return err
	}

	ip := binary.LittleEndian.Uint32(buf[0:4])
	fiveMinImpact := binary.LittleEndian.Uint32(buf[4:8])
	hourImpact := binary.LittleEndian.Uint32(buf[8:12])
	dayImpact := binary.LittleEndian.Uint32(buf[12:16])

	impacts := datastore.ImpactAmounts{
		FiveMin: datastore.ImpactAmount(fiveMinImpact),
		Hour:    datastore.ImpactAmount(hourImpact),
		Day:     datastore.ImpactAmount(dayImpact),
	}
	ipData := server.store.ForgiveIP(datastore.IPLong(ip), impacts)

	return writeIPData(ipData, conn)
}

// writeOK writes a single zero byte to the client to indicate success
func writeOK(conn io.ReadWriter) error {
	var buf [1]byte
	_, err := conn.Write(buf[0:])
	return err
}

func writeIPData(ipData datastore.IPData, conn io.ReadWriter) error {
	var buf [15]byte
	binary.LittleEndian.PutUint32(buf[0:4], uint32(ipData.MaxImpacts.FiveMin))
	binary.LittleEndian.PutUint32(buf[4:8], uint32(ipData.MaxImpacts.Hour))
	binary.LittleEndian.PutUint32(buf[8:12], uint32(ipData.MaxImpacts.Day))
	binary.LittleEndian.PutUint16(buf[12:14], uint16(ipData.Forgiven))
	buf[14] = ipData.BlackWhite

	_, err := conn.Write(buf[0:])
	return err
}

func (server *Server) persistEvery(interval time.Duration) {
	doEvery(interval, func() {
		log.Println("Starting background save...")
		err := server.store.Persist()
		if err != nil {
			log.Println(err)
		} else {
			log.Println("Background save finished")
		}
	})
}

func (server *Server) backupEvery(interval time.Duration) {
	doEvery(interval, func() {
		log.Println("Starting backup...")
		err := server.store.BackupToS3()
		if err != nil {
			log.Println(err)
		} else {
			log.Println("Backup finished")
		}
	})
}

func doEvery(interval time.Duration, fnc func()) {
	if interval == time.Duration(0) {
		return
	}

	go func() {
		for {
			timer := time.NewTimer(interval)
			<-timer.C
			fnc()
		}
	}()
}

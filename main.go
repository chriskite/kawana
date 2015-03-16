package main

import (
	"flag"
	"fmt"
	"github.com/chriskite/kawana/server"
	"github.com/rlmcpherson/s3gof3r"
	"log"
	"runtime"
)

type options struct {
	port            int
	dataDir         string
	s3Bucket        string
	persistInterval int
	backupInterval  int
	procs           int
}

func (o options) String() string {
	s := ""
	s += fmt.Sprintf("port: %d, ", o.port)
	s += fmt.Sprintf("dataDir: %s, ", o.dataDir)
	s += fmt.Sprintf("s3Bucket: %s, ", o.s3Bucket)
	s += fmt.Sprintf("persistInterval: %d, ", o.persistInterval)
	s += fmt.Sprintf("backupInterval: %d, ", o.backupInterval)
	s += fmt.Sprintf("procs: %d", o.procs)
	return s
}

func main() {
	port := flag.Int("port", 9291, "port number")
	dataDir := flag.String("dataDir", "/var/lib/kawana", "data directory")
	s3Bucket := flag.String("s3Bucket", "", "S3 bucket for backup")
	persistInterval := flag.Int("persist", 300, "persistence interval in seconds. 0 to disable")
	backupInterval := flag.Int("backup", 0, "backup interval in seconds. 0 to disable")
	procs := flag.Int("procs", 1, "GOMAXPROCS")
	flag.Parse()

	opts := options{
		port:            *port,
		dataDir:         *dataDir,
		s3Bucket:        *s3Bucket,
		persistInterval: *persistInterval,
		backupInterval:  *backupInterval,
		procs:           *procs,
	}

	log.Println("Kawana startup -", opts)

	runtime.GOMAXPROCS(opts.procs)

	if opts.backupInterval > 0 {
		if opts.s3Bucket != "" {
			err := testS3(opts.s3Bucket)
			if err != nil {
				log.Fatal(err)
			}
		} else {
			log.Fatal("Backup enabled but s3Bucket not specified")
		}
	}
	server := server.New(opts.port, opts.dataDir, opts.persistInterval, opts.backupInterval, opts.s3Bucket)
	server.Start()
}

func testS3(s3Bucket string) error {
	// test aws s3 connection
	k, err := s3gof3r.EnvKeys() // get S3 keys from environment
	if err != nil {
		return err
	}
	// Open bucket to put file into
	s3 := s3gof3r.New("", k)
	b := s3.Bucket(s3Bucket)
	// Open a PutWriter for upload
	_, err = b.PutWriter("kawana-test", nil, nil)
	if err != nil {
		return err
	}

	return nil
}

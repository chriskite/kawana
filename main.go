package main

import (
	"flag"
	"github.com/chriskite/kawana/server"
	"github.com/rlmcpherson/s3gof3r"
	"log"
	"runtime"
)

func main() {
	port := flag.Int("port", 9291, "port number")
	dataDir := flag.String("dataDir", "/var/lib/kawana", "data directory")
	s3Bucket := flag.String("s3Bucket", "", "S3 bucket for backup")
	persistInterval := flag.Int("persist", 300, "persistence interval in seconds. 0 to disable")
	backupInterval := flag.Int("backup", 0, "backup interval in seconds. 0 to disable")
	procs := flag.Int("procs", 1, "GOMAXPROCS")
	flag.Parse()

	runtime.GOMAXPROCS(*procs)

	if *backupInterval > 0 && *s3Bucket != "" {
		err := testS3(*s3Bucket)
		if err != nil {
			log.Fatal(err)
		}
	}
	server := server.New(*port, *dataDir, *persistInterval, *backupInterval, *s3Bucket)
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

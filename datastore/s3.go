package datastore

import (
	"errors"
	"github.com/rlmcpherson/s3gof3r"
	"io"
	"os"
)

// BackupToS3 writes the kawana.kdb to s3, overwriting an existing backup
// if one exists
func (store *IPDataStore) BackupToS3() error {
	if store.s3Bucket == "" {
		return errors.New("Empty s3Bucket name")
	}

	k, err := s3gof3r.EnvKeys() // get S3 keys from environment
	if err != nil {
		return err
	}
	// Open bucket to put file into
	s3 := s3gof3r.New("", k)
	b := s3.Bucket(store.s3Bucket)

	// open file to upload
	file, err := os.Open(store.kdbPath())
	if err != nil {
		return err
	}

	// Open a PutWriter for upload
	w, err := b.PutWriter(store.s3FilePath(), nil, nil)
	if err != nil {
		return err
	}
	if _, err = io.Copy(w, file); err != nil { // Copy into S3
		return err
	}
	if err = w.Close(); err != nil {
		return err
	}
	return nil
}

func (store *IPDataStore) s3FilePath() string {
	return kdbFile
}

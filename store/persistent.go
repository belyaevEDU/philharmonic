package store

import (
	"fmt"
	"log"
	"os"

	bolt "go.etcd.io/bbolt"
)

type TaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

func NewTaskStore(file string, mode os.FileMode, bucket string) (*TaskStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %s", file)
	}

	ts := TaskStore{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	err = ts.CreateBucket()
	if err != nil {
		log.Printf("bucket already exists, will use it instead of creating a new one") // ?
	}

	return &ts, nil
}

func (t *TaskStore) Close() error {
	err := t.Db.Close()
	if err != nil {
		return err
	}
	return nil
}

func (t *TaskStore) CreateBucket() error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(t.Bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", t.Bucket, err)
		}
		return nil
	})
}

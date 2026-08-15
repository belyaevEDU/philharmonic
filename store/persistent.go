package store

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

const boltOpenTimeout = 5 * time.Second

type TaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

var _ Store[task.Task] = (*TaskStore)(nil)

func NewBoltTaskStore(file string, mode os.FileMode, bucket string) (*TaskStore, error) {
	db, err := bolt.Open(file, mode, &bolt.Options{Timeout: boltOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("unable to open %s: %w", file, err)
	}

	ts := &TaskStore{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	if err := ts.CreateBucket(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return ts, nil
}

func (t *TaskStore) Close() error {
	return t.Db.Close()
}

func (t *TaskStore) CreateBucket() error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(t.Bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", t.Bucket, err)
		}
		return nil
	})
}

func (t *TaskStore) Put(key uuid.UUID, value *task.Task) error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))

		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}

		return b.Put([]byte(key[:]), buf)
	})
}

func (t *TaskStore) Get(key uuid.UUID) (*task.Task, error) {
	var out task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		raw := b.Get(key[:])
		if raw == nil {
			return fmt.Errorf("task %s: %w", key, ErrNotFound)
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return fmt.Errorf("unable to unmarshal: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (t *TaskStore) List() ([]*task.Task, error) {
	var tasks []*task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		return b.ForEach(func(k, v []byte) error {
			var out task.Task
			if err := json.Unmarshal(v, &out); err != nil {
				return err
			}
			tasks = append(tasks, &out)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return tasks, nil
}

func (t *TaskStore) Count() (int, error) {
	count := 0
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		return b.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	})
	if err != nil {
		return -1, err
	}
	return count, nil
}

type TaskEventStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

var _ Store[task.TaskEvent] = (*TaskEventStore)(nil)

func NewBoltTaskEventStore(file string, mode os.FileMode, bucket string) (*TaskEventStore, error) {
	db, err := bolt.Open(file, mode, &bolt.Options{Timeout: boltOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("unable to open %s: %w", file, err)
	}

	tes := &TaskEventStore{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	if err := tes.CreateBucket(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return tes, nil
}

func (tes *TaskEventStore) Close() error {
	return tes.Db.Close()
}

func (tes *TaskEventStore) CreateBucket() error {
	return tes.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(tes.Bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", tes.Bucket, err)
		}
		return nil
	})
}

func (tes *TaskEventStore) Put(key uuid.UUID, value *task.TaskEvent) error {
	return tes.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))

		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}

		return b.Put([]byte(key[:]), buf)
	})
}

func (tes *TaskEventStore) Get(key uuid.UUID) (*task.TaskEvent, error) {
	var out task.TaskEvent
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		raw := b.Get(key[:])
		if raw == nil {
			return fmt.Errorf("task event %s: %w", key, ErrNotFound)
		}
		if err := json.Unmarshal(raw, &out); err != nil {
			return fmt.Errorf("unable to unmarshal: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &out, nil
}

func (tes *TaskEventStore) List() ([]*task.TaskEvent, error) {
	var events []*task.TaskEvent
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		return b.ForEach(func(k, v []byte) error {
			var out task.TaskEvent
			if err := json.Unmarshal(v, &out); err != nil {
				return err
			}
			events = append(events, &out)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return events, nil
}

func (tes *TaskEventStore) Count() (int, error) {
	count := 0
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		return b.ForEach(func(k, v []byte) error {
			count++
			return nil
		})
	})
	if err != nil {
		return -1, err
	}
	return count, nil
}

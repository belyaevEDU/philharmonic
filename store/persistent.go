package store

import (
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/belyaevedu/philharmonic/task"
	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

type TaskStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

var _ Store[task.Task] = (*TaskStore)(nil)

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

func (t *TaskStore) Put(key uuid.UUID, value *task.Task) error {
	return t.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))

		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}

		err = b.Put([]byte(key[:]), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (t *TaskStore) Get(key uuid.UUID) (*task.Task, error) {
	var task task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		t := b.Get(key[:])
		if t == nil {
			return fmt.Errorf("task %s not found", key)
		}
		err := json.Unmarshal(t, &task)
		if err != nil {
			return fmt.Errorf("unable to unmarshal: %w", err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &task, nil
}

func (t *TaskStore) List() ([]*task.Task, error) {
	var tasks []*task.Task
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		b.ForEach(func(k, v []byte) error {
			var task task.Task
			err := json.Unmarshal(v, &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (t *TaskStore) Count() (int, error) {
	taskCount := 0
	err := t.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(t.Bucket))
		b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})

	if err != nil {
		return -1, err
	}

	return taskCount, nil
}

type TaskEventStore struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string
}

var _ Store[task.TaskEvent] = (*TaskEventStore)(nil)

func NewTaskEventStore(file string, mode os.FileMode, bucket string) (*TaskEventStore, error) {
	db, err := bolt.Open(file, mode, nil)
	if err != nil {
		return nil, fmt.Errorf("unable to open %s", file)
	}

	tes := TaskEventStore{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	err = tes.CreateBucket()
	if err != nil {
		log.Printf("bucket already exists, will use it instead of creating a new one") // ?
	}

	return &tes, nil
}

func (tes *TaskEventStore) Close() {
	tes.Db.Close()
}

func (tes *TaskEventStore) CreateBucket() error {
	return tes.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucket([]byte(tes.Bucket)) // CreateBucketNotExists is a thing, should refactor for that
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

		err = b.Put([]byte(key[:]), buf)
		if err != nil {
			return err
		}
		return nil
	})
}

func (tes *TaskEventStore) Get(key uuid.UUID) (*task.TaskEvent, error) {
	var taskEvent task.TaskEvent
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		t := b.Get(key[:])
		if t == nil {
			return fmt.Errorf("task %s not found", key)
		}
		err := json.Unmarshal(t, &taskEvent)
		if err != nil {
			return fmt.Errorf("unable to unmarshal: %w", err)
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return &taskEvent, nil
}

func (tes *TaskEventStore) List() ([]*task.TaskEvent, error) {
	var tasks []*task.TaskEvent
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		b.ForEach(func(k, v []byte) error {
			var task task.TaskEvent
			err := json.Unmarshal(v, &task)
			if err != nil {
				return err
			}
			tasks = append(tasks, &task)
			return nil
		})
		return nil
	})

	if err != nil {
		return nil, err
	}

	return tasks, nil
}

func (tes *TaskEventStore) Count() (int, error) {
	taskCount := 0
	err := tes.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(tes.Bucket))
		b.ForEach(func(k, v []byte) error {
			taskCount++
			return nil
		})
		return nil
	})

	if err != nil {
		return -1, err
	}

	return taskCount, nil
}

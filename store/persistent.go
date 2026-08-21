package store

import (
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/google/uuid"
	bolt "go.etcd.io/bbolt"
)

const boltOpenTimeout = 5 * time.Second

// BoltStore is a generic bbolt-backed implementation of Store[T]
// Values are json-marshalled into a single bucket,
// keyed by the 16 raw bytes of the value's uuid.
// In short, key: uuid.UUID; value: T
type BoltStore[T any] struct {
	Db       *bolt.DB
	DbFile   string
	FileMode os.FileMode
	Bucket   string

	// shared marks views created by Bucket over a SharedBolt.
	// if so, the store does not own the file handle
	// and Close is a no-op and the SharedBolt closes it
	shared bool
}

func NewBoltStore[T any](file string, mode os.FileMode, bucket string) (*BoltStore[T], error) {
	db, err := bolt.Open(file, mode, &bolt.Options{Timeout: boltOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("unable to open %s: %w", file, err)
	}

	s := &BoltStore[T]{
		Db:       db,
		DbFile:   file,
		FileMode: mode,
		Bucket:   bucket,
	}

	if err := s.CreateBucket(); err != nil {
		_ = db.Close()
		return nil, err
	}

	return s, nil
}

// owns a single bbolt file backing several bucket views.
// bbolt takes an exclusive lock on the file,
// so a component's stores must all come from one open handle.
// SharedBolt acts as that handle, and Bucket hands out
// Store[T] views over its buckets
type SharedBolt struct {
	Db     *bolt.DB
	DbFile string
}

func OpenSharedBolt(file string, mode os.FileMode) (*SharedBolt, error) {
	db, err := bolt.Open(file, mode, &bolt.Options{Timeout: boltOpenTimeout})
	if err != nil {
		return nil, fmt.Errorf("unable to open %s: %w", file, err)
	}
	return &SharedBolt{Db: db, DbFile: file}, nil
}

func Bucket[T any](s *SharedBolt, bucket string) (*BoltStore[T], error) {
	err := s.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", bucket, err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &BoltStore[T]{
		Db:     s.Db,
		DbFile: s.DbFile,
		Bucket: bucket,
		shared: true,
	}, nil
}

func (s *SharedBolt) Close() error {
	return s.Db.Close()
}

func (s *BoltStore[T]) Close() error {
	if s.shared {
		// the owning SharedBolt closes the file
		return nil
	}
	return s.Db.Close()
}

func (s *BoltStore[T]) CreateBucket() error {
	return s.Db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(s.Bucket))
		if err != nil {
			return fmt.Errorf("create bucket %s: %w", s.Bucket, err)
		}
		return nil
	})
}

func (s *BoltStore[T]) Put(key uuid.UUID, value *T) error {
	return s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))

		buf, err := json.Marshal(value)
		if err != nil {
			return err
		}

		return b.Put([]byte(key[:]), buf)
	})
}

func (s *BoltStore[T]) Get(key uuid.UUID) (*T, error) {
	var out T
	err := s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))
		raw := b.Get(key[:])
		if raw == nil {
			return fmt.Errorf("%w: %s", ErrNotFound, key)
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

func (s *BoltStore[T]) List() ([]*T, error) {
	var out []*T
	err := s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))
		return b.ForEach(func(k, v []byte) error {
			var item T
			if err := json.Unmarshal(v, &item); err != nil {
				return err
			}
			out = append(out, &item)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (s *BoltStore[T]) Count() (int, error) {
	count := 0
	err := s.Db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))
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

func (s *BoltStore[T]) Delete(key uuid.UUID) error {
	return s.Db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(s.Bucket))
		// bolt's Delete is a no-op on a missing key, which is the desired
		// behavior for an idempotent cleanup
		return b.Delete(key[:])
	})
}

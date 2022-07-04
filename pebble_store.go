package raftpebble

import (
	"bytes"
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

var (
	// Prefixes to use in lieu of bucket names, since pebble doesn't support buckets
	dbLogs = []byte("logs/")
	dbConf = []byte("conf/")

	// ErrKeyNotFound indicates a given key does not exist
	ErrKeyNotFound = errors.New("not found")
	writeOptions   = &pebble.WriteOptions{Sync: true}
)

// PebbleStore provides access to pebble for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type PebbleStore struct {
	// conn is the underlying handle to the db
	conn *pebble.DB

	// path is where pebble keeps its files
	path string
}

// Options contains all the configuration used to open the database
type Options struct {
	// Path is the file path
	Path string

	// PebbleOptions contains any specific pebble options you might
	// want to specify [e.g. open timeout]
	PebbleOptions *pebble.Options
}

// readOnly returns true if the contained pebble options say to open
// the DB in readOnly mode [this can be useful to tools that want
// to examine the log]
func (o *Options) readOnly() bool {
	return o != nil && o.PebbleOptions != nil && o.PebbleOptions.ReadOnly
}

// NewPebbleStore takes a path and returns a connected Raft backend.
func NewPebbleStore(path string) (*PebbleStore, error) {
	return New(Options{Path: path})
}

// New uses the supplied options to open the pebble db and prepare it for use as a raft backend.
func New(options Options) (*PebbleStore, error) {
	// Try to connect
	handle, err := pebble.Open(options.Path, options.PebbleOptions)
	if err != nil {
		return nil, err
	}

	// Create the new store
	store := &PebbleStore{
		conn: handle,
		path: options.Path,
	}

	return store, nil
}

// Close is used to gracefully close the DB connection.
func (b *PebbleStore) Close() error {
	return b.conn.Close()
}

func (b *PebbleStore) Metrics() *pebble.Metrics {
	return b.conn.Metrics()
}

// FirstIndex returns the first known index from the Raft log.
func (b *PebbleStore) FirstIndex() (uint64, error) {
	snap := b.conn.NewSnapshot()
	defer snap.Close()

	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: dbLogs,
	})
	defer iter.Close()

	if valid := iter.First(); valid {
		if key := iter.Key(); key != nil {
			return bytesToUint64(bytes.TrimPrefix(key, dbLogs)), nil
		}
	}

	return 0, nil
}

// LastIndex returns the last known index from the Raft log.
func (b *PebbleStore) LastIndex() (uint64, error) {
	snap := b.conn.NewSnapshot()
	defer snap.Close()

	iter := snap.NewIter(&pebble.IterOptions{
		LowerBound: dbLogs,
	})
	defer iter.Close()

	if valid := iter.Last(); valid {
		if key := iter.Key(); key != nil {
			return bytesToUint64(bytes.TrimPrefix(key, dbLogs)), nil
		}
	}

	return 0, nil
}

// GetLog is used to retrieve a log from pebble at a given index.
func (b *PebbleStore) GetLog(idx uint64, log *raft.Log) error {
	snap := b.conn.NewSnapshot()
	defer snap.Close()

	key := append(dbLogs, uint64ToBytes(idx)...)
	val, closer, err := snap.Get(key)

	if err != nil {
		if closer != nil {
			closer.Close()
		}

		if err == pebble.ErrNotFound {
			return raft.ErrLogNotFound
		} else {
			return err
		}
	}

	newVal := make([]byte, len(val))
	copy(newVal, val)

	err = decodeMsgPack(newVal, log)
	if closer != nil {
		closer.Close()
	}

	return err
}

// StoreLog is used to store a single raft log
func (b *PebbleStore) StoreLog(log *raft.Log) error {
	return b.StoreLogs([]*raft.Log{log})
}

// StoreLogs is used to store a set of raft logs
func (b *PebbleStore) StoreLogs(logs []*raft.Log) error {
	batch := b.conn.NewBatch()
	defer batch.Close()

	for _, log := range logs {
		key := append(dbLogs, uint64ToBytes(log.Index)...)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		if err := batch.Set(key, val.Bytes(), writeOptions); err != nil {
			return err
		}
	}

	return batch.Commit(writeOptions)
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *PebbleStore) DeleteRange(min, max uint64) error {
	minKey := append(dbLogs, uint64ToBytes(min)...)
	batch := b.conn.NewIndexedBatch()
	defer batch.Close()

	iter := batch.NewIter(nil)

	for iter.SeekGE(minKey); iter.Valid(); iter.Next() {
		k := iter.Key()

		// Handle out-of-range log index
		if bytesToUint64(bytes.TrimPrefix(k, dbLogs)) > max {
			break
		}

		// Delete in-range log index
		if err := batch.Delete(k, writeOptions); err != nil {
			iter.Close()
			return err
		}
	}

	iter.Close()
	return batch.Commit(writeOptions)
}

// Set is used to set a key/value set outside of the raft log
func (b *PebbleStore) Set(k, v []byte) error {
	key := append(dbConf, k...)
	return b.conn.Set(key, v, writeOptions)
}

// Get is used to retrieve a value from the k/v store by key
func (b *PebbleStore) Get(k []byte) ([]byte, error) {
	key := append(dbConf, k...)
	val, closer, err := b.conn.Get(key)

	if err != nil {
		if closer != nil {
			closer.Close()
		}

		if err == pebble.ErrNotFound {
			return nil, ErrKeyNotFound
		} else {
			return nil, err
		}
	}

	newVal := make([]byte, len(val))
	copy(newVal, val)
	result := append([]byte(nil), newVal...)

	if closer != nil {
		closer.Close()
	}

	return result, nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *PebbleStore) SetUint64(key []byte, val uint64) error {
	return b.conn.Set(key, uint64ToBytes(val), writeOptions)
}

// GetUint64 is like Get, but handles uint64 values
func (b *PebbleStore) GetUint64(key []byte) (uint64, error) {
	val, closer, err := b.conn.Get(key)

	if err != nil {
		if closer != nil {
			closer.Close()
		}

		if err == pebble.ErrNotFound {
			return 0, ErrKeyNotFound
		} else {
			return 0, err
		}
	}

	newVal := make([]byte, len(val))
	copy(newVal, val)
	result := bytesToUint64(newVal)
	if closer != nil {
		closer.Close()
	}

	return result, nil
}

// Sync performs an fsync on the database file handle. This is not necessary
// under normal operation unless NoSync is enabled, in which this forces the
// database file to sync against the disk.
func (b *PebbleStore) Sync() error {
	return b.conn.Flush()
}

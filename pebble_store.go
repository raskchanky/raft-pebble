package raftpebble

import (
	"errors"

	"github.com/cockroachdb/pebble"
	"github.com/hashicorp/raft"
)

const (
	// Permissions to use on the db file. This is only used if the
	// database file does not exist and needs to be created.
	dbFileMode = 0600
)

var (
	// An error indicating a given key does not exist
	ErrKeyNotFound = errors.New("not found")
)

// PebbleStore provides access to pebble for Raft to store and retrieve
// log entries. It also provides key/value storage, and can be used as
// a LogStore and StableStore.
type PebbleStore struct {
	// conn is the underlying handle to the db.
	conn *pebble.DB

	// The path to the pebble database directory
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

// readOnly returns true if the contained bolt options say to open
// the DB in readOnly mode [this can be useful to tools that want
// to examine the log]
func (o *Options) readOnly() bool {
	return o != nil && o.PebbleOptions != nil && o.PebbleOptions.ReadOnly
}

// NewPebbleStore takes a file path and returns a connected Raft backend.
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
	iter := b.conn.NewIter(nil)
	defer iter.Close()

	if valid := iter.First(); valid	{
		if key := iter.Key(); key != nil {
			return bytesToUint64(key), nil
		}
	}

	return 0, nil
}

// LastIndex returns the last known index from the Raft log.
func (b *PebbleStore) LastIndex() (uint64, error) {
	iter := b.conn.NewIter(nil)
	defer iter.Close()

	if valid := iter.Last(); valid	{
		if key := iter.Key(); key != nil {
			return bytesToUint64(key), nil
		}
	}

	return 0, nil
}

// GetLog is used to retrieve a log from pebble at a given index.
func (b *PebbleStore) GetLog(idx uint64, log *raft.Log) error {
	val, closer, err := b.conn.Get(uint64ToBytes(idx))

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

	err = decodeMsgPack(val, log)
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
	for _, log := range logs {
		key := uint64ToBytes(log.Index)
		val, err := encodeMsgPack(log)
		if err != nil {
			return err
		}
		// Note: this will sync to disk by default. Pass &pebble.WriteOptions{Sync: false}
		// to disable this and buffer writes in memory.
		if err := b.conn.Set(key, val.Bytes(), nil); err != nil {
			return err
		}
	}

	return nil
}

// DeleteRange is used to delete logs within a given range inclusively.
func (b *PebbleStore) DeleteRange(min, max uint64) error {
	minKey := uint64ToBytes(min)
	bt := b.conn.NewIndexedBatch()

	iter := bt.NewIter(nil)

	for iter.SeekGE(minKey); iter.Valid(); iter.Next() {
		k := iter.Key()

		// Handle out-of-range log index
		if bytesToUint64(k) > max {
			break
		}

		// Delete in-range log index
		if err := bt.Delete(k, nil); err != nil {
			iter.Close()
			return err
		}
	}

	iter.Close()
	return bt.Commit(nil)
}

// Set is used to set a key/value set outside of the raft log
func (b *PebbleStore) Set(k, v []byte) error {
	return b.conn.Set(k, v, nil)
}

// Get is used to retrieve a value from the k/v store by key
func (b *PebbleStore) Get(k []byte) ([]byte, error) {
	val, closer, err := b.conn.Get(k)

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

	result := append([]byte(nil), val...)

	if closer != nil {
		closer.Close()
	}

	return result, nil
}

// SetUint64 is like Set, but handles uint64 values
func (b *PebbleStore) SetUint64(key []byte, val uint64) error {
	return b.conn.Set(key, uint64ToBytes(val), nil)
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

	result := bytesToUint64(val)
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

package raftpebble

import (
	"os"
	"testing"

	"github.com/hashicorp/raft/bench"
)

func BenchmarkPebbleStore_FirstIndex(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.FirstIndex(b, store)
}

func BenchmarkPebbleStore_LastIndex(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.LastIndex(b, store)
}

func BenchmarkPebbleStore_GetLog(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetLog(b, store)
}

func BenchmarkPebbleStore_StoreLog(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLog(b, store)
}

func BenchmarkPebbleStore_StoreLogs(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.StoreLogs(b, store)
}

func BenchmarkPebbleStore_DeleteRange(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.DeleteRange(b, store)
}

func BenchmarkPebbleStore_Set(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Set(b, store)
}

func BenchmarkPebbleStore_Get(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.Get(b, store)
}

func BenchmarkPebbleStore_SetUint64(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.SetUint64(b, store)
}

func BenchmarkPebbleStore_GetUint64(b *testing.B) {
	store := testPebbleStore(b)
	defer store.Close()
	defer os.Remove(store.path)

	raftbench.GetUint64(b, store)
}

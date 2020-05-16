package kv

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"github.com/luoxiaojun1992/raftkv/kv/engines"
	"io"
)

type KV struct {
	Engine engines.Engine
}

func NewKV(engineType string, dataDir string) *KV {
	engine := engines.NewEngine(engineType, dataDir)
	return &KV{Engine: engine}
}

func (kv *KV) Close() error {
	return kv.Engine.Close()
}

func (kv *KV) Apply(log *raft.Log) interface{} {
	var entry map[string]string

	jsonErr := json.Unmarshal(log.Data, &entry)
	if jsonErr != nil {
		panic(jsonErr)
	}

	return kv.Engine.Set(entry["key"], entry["val"])
}

// Snapshot is used to support log compaction. This call should
// return an FSMSnapshot which can be used to save a point-in-time
// snapshot of the FSM. Apply and Snapshot are not called in multiple
// threads, but Apply will be called concurrently with Persist. This means
// the FSM should be implemented in a fashion that allows for concurrent
// updates while a snapshot is happening.
func (kv *KV) Snapshot() (raft.FSMSnapshot, error) {
	return NewKVSnapshot(kv.Engine.Snapshot()), nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (kv *KV) Restore(reader io.ReadCloser) error {
	return kv.Engine.Restore(reader)
}

type SnapshotWriter struct {
	Sink raft.SnapshotSink
}

func (sw SnapshotWriter) Write(p []byte) (n int, err error) {
	return sw.Sink.Write(p)
}

func NewSnapshotWriter(sink raft.SnapshotSink) *SnapshotWriter {
	return &SnapshotWriter{Sink: sink}
}

type Snapshot struct {
	EngineSnapshot engines.EngineSnapshot
}

func NewKVSnapshot(engineSnapshot engines.EngineSnapshot) *Snapshot {
	return &Snapshot{EngineSnapshot: engineSnapshot}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (kvSnapshot *Snapshot) Persist(sink raft.SnapshotSink) error {
	return kvSnapshot.EngineSnapshot.Persist(NewSnapshotWriter(sink))
}

// Release is invoked when we are finished with the snapshot.
func (kvSnapshot *Snapshot) Release() {

}

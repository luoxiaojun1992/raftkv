package kv

import (
	"encoding/json"
	"github.com/hashicorp/raft"
	"io"
)

type KV struct {
	Engine Engine
}

func NewKV(engineType string, dataDir string) *KV {
	engine := NewEngine(engineType, dataDir)
	return &KV{Engine: engine}
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
	return NewKVSnapshot(kv.Engine.GetData()), nil
}

// Restore is used to restore an FSM from a snapshot. It is not called
// concurrently with any other command. The FSM must discard all previous
// state.
func (kv *KV) Restore(reader io.ReadCloser) error {
	var data map[string]string
	jsonErr := json.NewDecoder(reader).Decode(&data)
	if jsonErr != nil {
		return jsonErr
	}

	return kv.Engine.SetData(data)
}

type Snapshot struct {
	data map[string]string
}

func NewKVSnapshot(data map[string]string) *Snapshot {
	return &Snapshot{data: data}
}

// Persist should dump all necessary state to the WriteCloser 'sink',
// and call sink.Close() when finished or call sink.Cancel() on error.
func (kvSnapshot *Snapshot) Persist(sink raft.SnapshotSink) error {
	jsonData, jsonErr := json.Marshal(kvSnapshot.data)
	if jsonErr != nil {
		return jsonErr
	}

	_, sinkErr := sink.Write(jsonData)
	if sinkErr != nil {
		return sinkErr
	}

	return nil
}

// Release is invoked when we are finished with the snapshot.
func (kvSnapshot *Snapshot) Release() {

}

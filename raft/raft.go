package raft

import (
	hasicorpRaft "github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
	"net"
	"os"
	"path/filepath"
	"time"
)

func NewRaftConfig (addr string) *hasicorpRaft.Config {
	raftConfig := hasicorpRaft.DefaultConfig()
	raftConfig.LocalID = hasicorpRaft.ServerID(addr)
	return raftConfig
}

func NewRaftTransport(addr string) hasicorpRaft.Transport {
	address, addErr := net.ResolveTCPAddr("tcp", addr)
	if addErr != nil {
		panic(addErr)
	}
	transport, tsErr := hasicorpRaft.NewTCPTransport(address.String(), address, 3, 10*time.Second, os.Stderr)
	if tsErr != nil {
		panic(tsErr)
	}

	return transport
}

func NewRaft(raftConfig *hasicorpRaft.Config, transport hasicorpRaft.Transport, fsm hasicorpRaft.FSM, dataDir string) *hasicorpRaft.Raft {
	logStore, logStoreErr := raftboltdb.NewBoltStore(filepath.Join("./data/"+dataDir, "raftleader-log.bolt"))
	if logStoreErr != nil {
		panic(logStoreErr)
	}

	stableStore, stableStoreErr := raftboltdb.NewBoltStore(filepath.Join("./data/"+dataDir, "raftleader-stable.bolt"))
	if stableStoreErr != nil {
		panic(stableStoreErr)
	}

	snapshotStore, snapshotStoreErr := hasicorpRaft.NewFileSnapshotStore("./data/"+dataDir+"/snapshot", 1, os.Stderr)
	if snapshotStoreErr != nil {
		panic(stableStoreErr)
	}

	r, raftErr := hasicorpRaft.NewRaft(raftConfig, fsm, logStore, stableStore, snapshotStore, transport)
	if raftErr != nil {
		panic(raftErr)
	}

	return r
}

func BootStrap(r *hasicorpRaft.Raft, conf *hasicorpRaft.Config, transport hasicorpRaft.Transport) {
	configuration := hasicorpRaft.Configuration{
		Servers: []hasicorpRaft.Server{
			{
				ID:      conf.LocalID,
				Address: transport.LocalAddr(),
			},
		},
	}
	r.BootstrapCluster(configuration)
}

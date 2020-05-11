package main

import (
	"fmt"
	roykv "github.com/luoxiaojun1992/raftkv/kv"
	royraft "github.com/luoxiaojun1992/raftkv/raft"
	"os"
	"time"
)

func main () {
	addr := os.Args[1]

	kv := roykv.NewKV()

	raftConfig := royraft.NewRaftConfig(addr)
	raftTransport := royraft.NewRaftTransport(addr)
	raft := royraft.NewRaft(raftConfig, raftTransport, kv)

	royraft.BootStrap(raft, raftConfig, raftTransport)

	time.Sleep(10 * time.Second)

	raft.Apply([]byte("foo"), 5*time.Second)

	time.Sleep(10 * time.Second)

	fmt.Println(kv.Data["foo"])
}

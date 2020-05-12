package main

import (
	hashicorpRaft "github.com/hashicorp/raft"
	roykv "github.com/luoxiaojun1992/raftkv/kv"
	royraft "github.com/luoxiaojun1992/raftkv/raft"
	"github.com/luoxiaojun1992/raftkv/services"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	pb "github.com/luoxiaojun1992/raftkv/pb"
)

//go:generate protoc -I ./protos --go_out=plugins=grpc:./pb ./protos/kv.proto
func main () {
	raftAddr := os.Args[1]
	grpcPort := os.Args[2]

	kv := roykv.NewKV()

	raft := startRaft(true, raftAddr, kv)

	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVServer(s, services.NewKvService(kv, raft))
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func startRaft(isLeader bool, raftAddr string, kv *roykv.KV) *hashicorpRaft.Raft {
	raftConfig := royraft.NewRaftConfig(raftAddr)
	raftTransport := royraft.NewRaftTransport(raftAddr)
	raft := royraft.NewRaft(raftConfig, raftTransport, kv)

	if isLeader {
		royraft.BootStrap(raft, raftConfig, raftTransport)
	}

	return raft
}

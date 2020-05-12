package main

import (
	"context"
	hashicorpRaft "github.com/hashicorp/raft"
	roykv "github.com/luoxiaojun1992/raftkv/kv"
	pb "github.com/luoxiaojun1992/raftkv/pb"
	royraft "github.com/luoxiaojun1992/raftkv/raft"
	"github.com/luoxiaojun1992/raftkv/services"
	"google.golang.org/grpc"
	"log"
	"net"
	"os"
	"time"
)

//go:generate protoc -I ./protos --go_out=plugins=grpc:./pb ./protos/kv.proto
//go:generate protoc -I ./protos --go_out=plugins=grpc:./pb ./protos/raftleader.proto
func main () {
	raftAddr := os.Args[1]
	grpcPort := os.Args[2]
	isLeader := os.Args[3]
	dataDir := os.Args[4]
	raftLeaderGrpcPort := ""
	if len(os.Args) >= 6 {
		raftLeaderGrpcPort = os.Args[5]
	}

	kv := roykv.NewKV()

	r := startRaft(isLeader == "1", raftAddr, raftLeaderGrpcPort, kv, dataDir)

	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVServer(s, services.NewKvService(kv, r))
	pb.RegisterRaftServer(s, services.NewRaftService(r))
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func startRaft(isLeader bool, raftAddr string, raftLeaderGrpcPort string, kv *roykv.KV, dataDir string) *hashicorpRaft.Raft {
	raftConfig := royraft.NewRaftConfig(raftAddr)
	raftTransport := royraft.NewRaftTransport(raftAddr)
	raft := royraft.NewRaft(raftConfig, raftTransport, kv, dataDir)

	if isLeader {
		royraft.BootStrap(raft, raftConfig, raftTransport)
	} else {
		// Set up a connection to the server.
		conn, err := grpc.Dial(raftLeaderGrpcPort, grpc.WithInsecure(), grpc.WithBlock())
		if err != nil {
			log.Printf("could not add node, did not connect: %v", err)
		}
		defer conn.Close()
		c := pb.NewRaftClient(conn)

		addNodeCtx, addNodeCancel := context.WithTimeout(context.TODO(), 10 * time.Second)
		defer addNodeCancel()

		addNodeReply, errAddNode := c.AddNode(addNodeCtx, &pb.AddNodeRequest{NodeAddr: raftAddr})
		if errAddNode != nil {
			log.Printf("could not add node: %v", errAddNode)
		}

		if !addNodeReply.GetResult() {
			log.Printf("could not add node: %v", errAddNode)
		}
	}

	return raft
}

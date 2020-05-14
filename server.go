package main

import (
	"context"
	"encoding/json"
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
//go:generate protoc -I ./protos --go_out=plugins=grpc:./pb ./protos/raft.proto
func main () {
	raftAddr := os.Args[1]
	grpcPort := os.Args[2]
	isLeader := os.Args[3]
	dataDir := os.Args[4]
	raftLeaderGrpcPort := ""
	if len(os.Args) >= 6 {
		raftLeaderGrpcPort = os.Args[5]
	}
	engineType := "memory"
	if len(os.Args) >= 7 {
		raftLeaderGrpcPort = os.Args[6]
	}

	kv := roykv.NewKV(engineType)

	r := startRaft(isLeader == "1", raftAddr, raftLeaderGrpcPort, kv, dataDir)

	//Broadcast leader grpc port
	if r.State() == hashicorpRaft.Leader {
		broadcastLeaderGrpcPort(r, grpcPort)
	}
	monitorLeaderChange(r, raftAddr, grpcPort)

	lis, err := net.Listen("tcp", grpcPort)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	s := grpc.NewServer()
	pb.RegisterKVServer(s, services.NewKvService(kv, r))
	pb.RegisterRaftServer(s, services.NewRaftService(kv, r))
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

func monitorLeaderChange(r *hashicorpRaft.Raft, raftAddr string, grpcPort string) {
	observationCh := make(chan hashicorpRaft.Observation)
	observer := hashicorpRaft.NewObserver(observationCh, true, func(o *hashicorpRaft.Observation) bool {
		_, ok := o.Data.(hashicorpRaft.LeaderObservation)
		if ok {
			return true
		} else {
			return false
		}
	})
	r.RegisterObserver(observer)
	go func() {
		for ; true; {
			observation := <-observationCh
			leaderObservation := observation.Data.(hashicorpRaft.LeaderObservation)
			if hashicorpRaft.ServerAddress(raftAddr) == leaderObservation.Leader {
				log.Println("Observed leader:" + leaderObservation.Leader)
				broadcastLeaderGrpcPort(r, grpcPort)
			}
		}
	}()
}

func broadcastLeaderGrpcPort(r *hashicorpRaft.Raft, grpcPort string) {
	var entry map[string]string
	entry = make(map[string]string)
	entry["key"] = "raftLeaderGrpcPort"
	entry["val"] = grpcPort

	jsonEntry, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		log.Println(jsonErr)
	} else {
		applyResult := r.Apply(jsonEntry, 10*time.Second)
		applyErr := applyResult.Error()
		if applyErr != nil {
			log.Println(applyErr)
		}
	}
}

func startRaft(isLeader bool, raftAddr string, raftLeaderGrpcPort string, kv *roykv.KV, dataDir string) *hashicorpRaft.Raft {
	raftConfig := royraft.NewRaftConfig(raftAddr)
	raftTransport := royraft.NewRaftTransport(raftAddr)
	raft := royraft.NewRaft(raftConfig, raftTransport, kv, dataDir)

	if isLeader {
		royraft.BootStrap(raft, raftConfig, raftTransport)
	} else {
		addNodeReply, errAddNode := registerFollower(raftLeaderGrpcPort, raftAddr)

		if errAddNode != nil {
			if addNodeReply != nil && addNodeReply.GetNotLeader() {
				addNodeReply2, errAddNode2 := registerFollower(addNodeReply.GetLeaderGrpcPort(), raftAddr)
				if errAddNode2 != nil {
					log.Printf("could not add node twice: %v", errAddNode)
				} else {
					if !addNodeReply2.GetResult() {
						log.Printf("could not add node twice: %v", errAddNode)
					}
				}
			}
			log.Printf("could not add node: %v", errAddNode)
		} else {
			if !addNodeReply.GetResult() {
				log.Printf("could not add node: %v", errAddNode)
			}
		}
	}

	return raft
}

func registerFollower(raftLeaderGrpcPort string, raftAddr string) (*pb.AddNodeReply, error) {
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

	return addNodeReply, errAddNode
}

package services

import (
	"context"
	"errors"
	hashicorpRaft "github.com/hashicorp/raft"
	roykv "github.com/luoxiaojun1992/raftkv/kv"
	raftkv "github.com/luoxiaojun1992/raftkv/pb"
	"time"
)

type RaftService struct {
	Kv   *roykv.KV
	Raft *hashicorpRaft.Raft
}

func (rs *RaftService) AddNode(ctx context.Context, req *raftkv.AddNodeRequest) (*raftkv.AddNodeReply, error) {
	if rs.Raft.State() != hashicorpRaft.Leader {
		leaderGrpcPort, grpcPortErr := rs.Kv.Engine.Get("raftLeaderGrpcPort")
		if grpcPortErr != nil {
			return &raftkv.AddNodeReply{
				Result:         false,
				NotLeader:      true,
				LeaderGrpcPort: "",
			}, grpcPortErr
		}

		return &raftkv.AddNodeReply{
			Result:         false,
			NotLeader:      true,
			LeaderGrpcPort: leaderGrpcPort,
		}, errors.New("NotLeader:" + string(rs.Raft.Leader()))
	}

	nodeAddr := req.GetNodeAddr()
	addRes := rs.Raft.AddVoter(hashicorpRaft.ServerID(nodeAddr), hashicorpRaft.ServerAddress(nodeAddr), 0, 10*time.Second)
	addErr := addRes.Error()
	if addErr != nil {
		return &raftkv.AddNodeReply{Result: false, NotLeader: false, LeaderGrpcPort: ""}, addErr
	}

	return &raftkv.AddNodeReply{Result: true, NotLeader: false, LeaderGrpcPort: ""}, nil
}

func NewRaftService(kv *roykv.KV, raft *hashicorpRaft.Raft) *RaftService {
	return &RaftService{Kv: kv, Raft: raft}
}

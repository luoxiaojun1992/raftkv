package services

import (
	"context"
	"errors"
	hashicorpRaft "github.com/hashicorp/raft"
	raftkv "github.com/luoxiaojun1992/raftkv/pb"
	"time"
)

type RaftService struct {
	Raft *hashicorpRaft.Raft
}

func (rs *RaftService) AddNode(ctx context.Context, req *raftkv.AddNodeRequest) (*raftkv.AddNodeReply, error) {
	if rs.Raft.State() != hashicorpRaft.Leader {
		return &raftkv.AddNodeReply{Result: false}, errors.New("NotLeader:" + string(rs.Raft.Leader()))
	}

	nodeAddr := req.GetNodeAddr()
	addRes := rs.Raft.AddVoter(hashicorpRaft.ServerID(nodeAddr), hashicorpRaft.ServerAddress(nodeAddr), 0, 10 * time.Second)
	addErr := addRes.Error()
	if addErr != nil {
		return &raftkv.AddNodeReply{Result: false}, addErr
	}

	return &raftkv.AddNodeReply{Result: true}, nil
}

func NewRaftService(raft *hashicorpRaft.Raft) *RaftService {
	return &RaftService{Raft: raft}
}

package services

import (
	"context"
	"encoding/json"
	"errors"
	hashicorpRaft "github.com/hashicorp/raft"
	roykv "github.com/luoxiaojun1992/raftkv/kv"
	raftkv "github.com/luoxiaojun1992/raftkv/pb"
	"time"
)

type KvService struct {
	Kv   *roykv.KV
	Raft *hashicorpRaft.Raft
	GrpcPort string
	LeaderGrpcPort string
}

func NewKvService(kv *roykv.KV, raft *hashicorpRaft.Raft, grpcPort, leaderGrpcPort string) *KvService {
	return &KvService{Kv: kv, Raft: raft, GrpcPort: grpcPort, LeaderGrpcPort: leaderGrpcPort}
}

func (kvs *KvService) Set(ctx context.Context, req *raftkv.SetRequest) (*raftkv.SetReply, error) {
	if kvs.Raft.State() != hashicorpRaft.Leader {
		leaderGrpcPort, grpcPortErr := kvs.Kv.Engine.Get("raftLeaderGrpcPort")
		if grpcPortErr != nil {
			if len(kvs.LeaderGrpcPort) > 0 {
				return &raftkv.SetReply{
					Result:         false,
					NotLeader:      true,
					LeaderGrpcPort: kvs.LeaderGrpcPort,
				}, errors.New("NotLeader:" + string(kvs.Raft.Leader()))
			}

			return &raftkv.SetReply{
				Result:         false,
				NotLeader:      true,
				LeaderGrpcPort: "",
			}, grpcPortErr
		}

		return &raftkv.SetReply{
			Result:         false,
			NotLeader:      true,
			LeaderGrpcPort: leaderGrpcPort,
		}, errors.New("NotLeader:" + string(kvs.Raft.Leader()))
	}

	key := req.GetKey()
	val := req.GetValue()

	var entry map[string]string
	entry = make(map[string]string)
	entry["key"] = key
	entry["val"] = val

	jsonEntry, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		return &raftkv.SetReply{
			Result:         false,
			NotLeader:      false,
			LeaderGrpcPort: "",
		}, jsonErr
	}

	applyResult := kvs.Raft.Apply(jsonEntry, 10*time.Second)

	applyErr := applyResult.Error()
	if applyErr != nil {
		return &raftkv.SetReply{
			Result:         false,
			NotLeader:      false,
			LeaderGrpcPort: "",
		}, applyErr
	}

	return &raftkv.SetReply{
		Result:         true,
		NotLeader:      false,
		LeaderGrpcPort: "",
	}, nil
}

func (kvs *KvService) Get(ctx context.Context, req *raftkv.GetRequest) (*raftkv.GetReply, error) {
	if kvs.Raft.State() != hashicorpRaft.Leader {
		leaderGrpcPort, grpcPortErr := kvs.Kv.Engine.Get("raftLeaderGrpcPort")
		if grpcPortErr != nil {
			if len(kvs.LeaderGrpcPort) > 0 {
				return &raftkv.GetReply{
					Value:         "",
					NotLeader:      true,
					LeaderGrpcPort: kvs.LeaderGrpcPort,
				}, errors.New("NotLeader:" + string(kvs.Raft.Leader()))
			}

			return &raftkv.GetReply{
				Value:          "",
				NotLeader:      true,
				LeaderGrpcPort: "",
			}, grpcPortErr
		}

		return &raftkv.GetReply{
			Value:          "",
			NotLeader:      true,
			LeaderGrpcPort: leaderGrpcPort,
		}, errors.New("NotLeader:" + string(kvs.Raft.Leader()))
	}

	key := req.GetKey()
	val, getErr := kvs.Kv.Engine.Get(key)
	if getErr == nil {
		return &raftkv.GetReply{
			Value:          val,
			NotLeader:      false,
			LeaderGrpcPort: "",
		}, nil
	} else {
		return &raftkv.GetReply{
			Value:          "",
			NotLeader:      false,
			LeaderGrpcPort: "",
		}, getErr
	}
}

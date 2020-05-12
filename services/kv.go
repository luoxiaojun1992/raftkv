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
	Kv *roykv.KV
	Raft *hashicorpRaft.Raft
}

func NewKvService(kv *roykv.KV, raft *hashicorpRaft.Raft) *KvService {
	return &KvService{Kv: kv, Raft: raft}
}

func (kvs *KvService) Set(ctx context.Context, req *raftkv.SetRequest) (*raftkv.SetReply, error) {
	key := req.GetKey()
	val := req.GetValue()

	var entry map[string]string
	entry = make(map[string]string)
	entry["key"] = key
	entry["val"] = val

	jsonEntry, jsonErr := json.Marshal(entry)
	if jsonErr != nil {
		return &raftkv.SetReply{
			Result:               false,
		}, jsonErr
	}

	applyResult := kvs.Raft.Apply(jsonEntry, 10*time.Second)

	applyErr := applyResult.Error()
	if applyErr != nil {
		return &raftkv.SetReply{
			Result:               false,
		}, applyErr
	}

	return &raftkv.SetReply{
		Result:               true,
	}, nil
}

func (kvs *KvService) Get(ctx context.Context, req *raftkv.GetRequest) (*raftkv.GetReply, error) {
	key := req.GetKey()
	val, existed := kvs.Kv.Data[key]
	if existed {
		return &raftkv.GetReply{
			Value:                val,
		}, nil
	} else {
		return nil, errors.New("Value of key (" + key + ") not found")
	}
}

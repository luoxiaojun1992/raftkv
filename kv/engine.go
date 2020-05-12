package kv

import "github.com/luoxiaojun1992/raftkv/kv/engines"

type Engine interface {
	Set(key string, value string) error
	Get(key string) (string, error)
	GetData() map[string]string
	SetData(data map[string]string) error
}

func NewEngine() Engine {
	data := make(map[string]string)
	return &engines.MemoryEngine{Data: data}
}

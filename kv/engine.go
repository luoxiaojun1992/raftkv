package kv

import (
	"github.com/luoxiaojun1992/raftkv/kv/engines"
	"log"
)

type Engine interface {
	Set(key string, value string) error
	Get(key string) (string, error)
	GetData() map[string]string
	SetData(data map[string]string) error
	Close() error
}

func NewEngine(engineType string, dataDir string) Engine {
	switch engineType {
	case "memory":
		log.Println("Memory Engine Started")
		return engines.NewMemoryEngine()
	case "badger":
		log.Println("Badger Engine Started")
		return engines.NewBadgerEngine(dataDir)
	default:
		log.Println("Memory Engine Started")
		return engines.NewMemoryEngine()
	}
}

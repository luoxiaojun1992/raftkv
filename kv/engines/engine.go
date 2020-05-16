package engines

import (
	"io"
	"log"
)

type Engine interface {
	Set(key string, value string) error
	Get(key string) (string, error)
	GetData() map[string]string
	SetData(data map[string]string) error
	MergeData(data map[string]string) error
	Snapshot() EngineSnapshot
	Restore(reader io.Reader) error
	Close() error
}

func NewEngine(engineType string, dataDir string) Engine {
	switch engineType {
	case "badger":
		log.Println("Badger Engine Started")
		return NewBadgerEngine(dataDir)
	default:
		log.Println("Badger Engine Started")
		return NewBadgerEngine(dataDir)
	}
}

type EngineSnapshot interface {
	Persist(writer io.Writer) error
}

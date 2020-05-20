package engines

import (
	"github.com/dgraph-io/badger"
	"io"
)

type BadgerSnapshot struct {
	DB *badger.DB
}

func NewBadgerSnapshot(db *badger.DB) *BadgerSnapshot {
	return &BadgerSnapshot{DB: db}
}

func (bs *BadgerSnapshot) Persist(writer io.Writer) error {
	_, errBak := bs.DB.Backup(writer, 0)
	return errBak
}

type BadgerEngine struct {
	DB *badger.DB
}

func NewBadgerEngine(dataDir string) *BadgerEngine {
	db, dbOpenErr := badger.Open(badger.DefaultOptions("./data/badger_" + dataDir))
	if dbOpenErr != nil {
		panic(dbOpenErr)
	}

	return &BadgerEngine{DB: db}
}

func (b *BadgerEngine) Set(key string, value string) error {
	return b.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})
}

func (b *BadgerEngine) Get(key string) (string, error) {
	var valCopy []byte

	viewErr := b.DB.View(func(txn *badger.Txn) error {
		item, getErr := txn.Get([]byte(key))
		if getErr != nil {
			return getErr
		} else {
			var valCopyErr error
			valCopy, valCopyErr = item.ValueCopy(nil)
			return valCopyErr
		}
	})

	if viewErr != nil {
		return "", viewErr
	}

	return string(valCopy), nil
}

func (b *BadgerEngine) GetData() map[string]string {
	data := make(map[string]string)
	return data
}

func (b *BadgerEngine) SetData(data map[string]string) error {
	return nil
}

func (b *BadgerEngine) MergeData(data map[string]string) error {
	return nil
}

func (b *BadgerEngine) Snapshot() EngineSnapshot {
	return NewBadgerSnapshot(b.DB)
}

func (b *BadgerEngine) Restore(reader io.Reader) error {
	return b.DB.Load(reader, 10)
}

func (b *BadgerEngine) Close() error {
	return b.DB.Close()
}

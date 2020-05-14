package engines

import "github.com/dgraph-io/badger"

type BadgerEngine struct {
	DB *badger.DB
}

func NewBadgerEngine(dataDir string) *BadgerEngine {
	db, dbOpenErr := badger.Open(badger.DefaultOptions("./data/badger_" + dataDir))
	if dbOpenErr != nil {
		panic(dbOpenErr)
	}

	return &BadgerEngine{DB:db}
}

func (m *BadgerEngine) Set(key string, value string) error {
	return m.DB.Update(func(txn *badger.Txn) error {
		err := txn.Set([]byte(key), []byte(value))
		return err
	})
}

func (m *BadgerEngine) Get(key string) (string, error) {
	var valCopy []byte

	viewErr := m.DB.View(func(txn *badger.Txn) error {
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

func (m *BadgerEngine) GetData() map[string]string {
	data := make(map[string]string)
	return data
}

func (m *BadgerEngine) SetData(data map[string]string) error {
	return nil
}

func (m *BadgerEngine) Close() error {
	return m.DB.Close()
}

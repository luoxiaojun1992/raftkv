package engines

import "errors"

type MemoryEngine struct {
	Data map[string]string
}

func (m *MemoryEngine) Set(key string, value string) error {
	m.Data[key] = value
	return nil
}

func (m *MemoryEngine) Get(key string) (string, error) {
	val, ok := m.Data[key]
	if !ok {
		return "", errors.New("Value of key (" + key + ") not found")
	}

	return val, nil
}

func (m *MemoryEngine) GetData() map[string]string {
	return m.Data
}

func (m *MemoryEngine) SetData(data map[string]string) error {
	m.Data = data
	return nil
}

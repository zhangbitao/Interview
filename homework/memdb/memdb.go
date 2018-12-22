package memdb

import (
	"encoding/binary"
	"errors"
	"io"
	"os"
	"sync"
	"sync/atomic"
)

var (
	ErrNotExist = errors.New("key is not exits")
	ErrRetry    = errors.New("memdb index is creating, please retry again after a while")
)

type MemDBOptions struct {
	Path     string
	HashSize uint64
}

func newMemDBOptions(path string) *MemDBOptions {
	return &MemDBOptions{
		Path: path,
	}
}

type MemDB struct {
	mu        sync.RWMutex
	o         *MemDBOptions
	datafd    *os.File
	memIndex  map[uint32]uint32 // hash key => data position
	isWriting uint32
}

func NewMemDB(o *MemDBOptions) *MemDB {
	return &MemDB{
		o:        o,
		memIndex: make(map[uint32]uint32, o.HashSize),
	}
}

func (m *MemDB) Open() error {
	if m.o == nil {
		return errors.New("options can't nil")
	}
	err := m.openDataFile()
	if err != nil {
		return err
	}

	go m.createMemIndex()

	return nil
}

func (m *MemDB) Close() {
	m.closeDataFile()
}

func (m *MemDB) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return []byte{}, errors.New("key is empty")
	}

	var (
		pos   uint32
		exist bool
	)

	hashKey := m.hash(key)
	if pos, exist = m.memIndex[hashKey]; !exist {
		if atomic.LoadUint32(&m.isWriting) > 0 {
			return []byte{}, ErrRetry
		}
		return []byte{}, ErrNotExist
	}

	return m.get(pos)
}

func (m *MemDB) Set(key []byte, value []byte) error {
	hashKey := m.hash(key)
	pos, err := m.dataSize()
	if err != nil {
		return err
	}

	if err := m.set(key, value); err != nil {
		return err
	}

	pos += 4 + uint32(len(key)) + 4
	m.memIndex[hashKey] = pos

	return nil
}

func (m *MemDB) set(key []byte, value []byte) error {
	if err := m.write(key); err != nil {
		return err
	}
	if err := m.write(value); err != nil {
		return err
	}
	return nil
}

func (m *MemDB) createMemIndex() {
	var (
		size uint32
		pos  uint32
	)
	atomic.StoreUint32(&m.isWriting, 1)
	defer func() {
		atomic.StoreUint32(&m.isWriting, 0)
	}()

	for {
		size = 4
		sizeByte, err := m.read(size, pos) // pos = start, size = 4
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("error")
		}

		size := binary.BigEndian.Uint32(sizeByte)
		pos += 4
		buf, err := m.read(size, pos+4) // pos = start + key_size, size = key + value_size
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("error")
		}

		key := buf[:len(buf)-4]
		valueSize := buf[len(buf)-4:]

		hashKey := m.hash(key)
		pos += uint32(len(buf)) // pos = start + key_size + key + value_size
		m.memIndex[hashKey] = pos

		pos += binary.BigEndian.Uint32(valueSize) // pos = start + key_size + key + value_size + value = next start
	}
}

func (m *MemDB) get(pos uint32) (value []byte, err error) {
	sizeByte, err := m.read(4, pos)
	if err != nil {
		return []byte{}, err
	}

	size := binary.BigEndian.Uint32(sizeByte)
	return m.read(size, pos+4)
}

func (m *MemDB) read(size uint32, pos uint32) (data []byte, err error) {
	data = make([]byte, size)

	m.mu.RLock()
	n, err := m.datafd.ReadAt(data, int64(pos))
	m.mu.RUnlock()
	if err != nil {
		return []byte{}, err
	}
	if uint32(n) != size {
		return []byte{}, errors.New("failed to read value")
	}

	return
}

func (m *MemDB) write(data []byte) error {
	buf := make([]byte, 4)
	binary.PutVarint(buf, int64(len(data)))

	m.mu.Lock()
	defer m.mu.Unlock()

	n, err := m.datafd.Write(buf)
	if err != nil {
		return err
	}
	if n != 4 {
		return errors.New("Failed to write value size")
	}

	n, err = m.datafd.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return errors.New("Failed to write value")
	}
	return nil
}

func (m *MemDB) hash(key []byte) uint32 {
	return Hash(key, 0xf00)
}

func (m *MemDB) openDataFile() error {
	datafile, err := os.OpenFile(m.o.Path, os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		return err
	}

	m.datafd = datafile

	return nil
}

func (m *MemDB) closeDataFile() {
	m.datafd.Close()
}

func (m *MemDB) dataSize() (size uint32, err error) {
	finfo, err := m.datafd.Stat()
	if err != nil {
		return 0, err
	}

	return uint32(finfo.Size()), nil
}

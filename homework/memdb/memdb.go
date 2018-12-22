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

// MemDBOptions is option of MemDB
type MemDBOptions struct {
	Path     string
	HashSize uint64
}

// NewMemDBOptions creates a new MemDBOptions
func NewMemDBOptions(path string, hashSize uint64) *MemDBOptions {
	return &MemDBOptions{
		Path:     path,
		HashSize: hashSize,
	}
}

// isValid checks options
func (o *MemDBOptions) isValid() error {
	if o.Path == "" {
		return errors.New("file path is empty")
	}
	if o.HashSize <= 0 {
		return errors.New("hash size must bt greater than 0")
	}

	return nil
}

// MemDB is in-memory key/value index
type MemDB struct {
	mu        sync.RWMutex
	o         *MemDBOptions
	datafd    *os.File
	mapLock   sync.Mutex
	memIndex  map[uint32]uint32 // hash key => data position
	isWriting uint32
}

// NewMemDB creates a MemDB struct
func NewMemDB(o *MemDBOptions) *MemDB {
	return &MemDB{
		o:        o,
		memIndex: make(map[uint32]uint32, o.HashSize),
	}
}

// Open opens data file, and creates memory index
func (m *MemDB) Open() error {
	if err := m.o.isValid(); err != nil {
		return err
	}

	if err := m.openDataFile(); err != nil {
		return err
	}

	go m.createMemIndex()

	return nil
}

// Close closes data file
func (m *MemDB) Close() {
	m.closeDataFile()
}

// Get returns the value for given key
func (m *MemDB) Get(key []byte) (value []byte, err error) {
	if len(key) == 0 {
		return []byte{}, errors.New("key is empty")
	}

	var (
		pos   uint32
		exist bool
	)

	hashKey := m.hash(key)
	m.mapLock.Lock()
	pos, exist = m.memIndex[hashKey]
	m.mapLock.Unlock()
	if !exist {
		if atomic.LoadUint32(&m.isWriting) > 0 {
			return []byte{}, ErrRetry
		}
		return []byte{}, ErrNotExist
	}

	return m.get(pos)
}

// Set writes key and value to data file, and add index in memory
func (m *MemDB) Set(key []byte, value []byte) error {
	hashKey := m.hash(key)
	pos, err := m.dataSize()
	if err != nil {
		return err
	}

	if err := m.set(key, value, int64(pos)); err != nil {
		return err
	}

	pos += 4 + uint32(len(key))

	m.mapLock.Lock()
	m.memIndex[hashKey] = pos
	m.mapLock.Unlock()

	return nil
}

// set sets key and value data to data file
func (m *MemDB) set(key []byte, value []byte, offset int64) error {
	if err := m.write(key, offset); err != nil {
		return err
	}
	valueOffset := offset + 4 + int64(len(key))
	if err := m.write(value, valueOffset); err != nil {
		return err
	}
	return nil
}

// createMemIndex creates index read data from data file
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

		size := binary.LittleEndian.Uint32(sizeByte)
		pos += 4
		buf, err := m.read(size+4, pos) // pos = start + key_size, size = key + value_size
		if err != nil {
			if err == io.EOF {
				break
			}
			panic("error")
		}

		key := buf[:len(buf)-4]
		valueSize := buf[len(buf)-4:]

		hashKey := m.hash(key)
		m.mapLock.Lock()
		m.memIndex[hashKey] = pos + uint32(len(buf)) - 4
		m.mapLock.Unlock()

		pos += uint32(len(buf)) + binary.LittleEndian.Uint32(valueSize) // pos = start + key_size + key + value_size + value = next start
	}
}

// get returns value for a given position
func (m *MemDB) get(pos uint32) (value []byte, err error) {
	sizeByte, err := m.read(4, pos)
	if err != nil {
		return []byte{}, err
	}

	size := binary.LittleEndian.Uint32(sizeByte)

	value, err = m.read(size, pos+4)
	if err == io.EOF {
		err = nil
	}
	return
}

// read reads `size` bytes from data file in `pos` offset
func (m *MemDB) read(size uint32, pos uint32) (data []byte, err error) {
	data = make([]byte, size)

	m.mu.RLock()
	n, err := m.datafd.ReadAt(data, int64(pos))
	m.mu.RUnlock()

	if err != nil {
		return
	}
	if uint32(n) != size {
		return []byte{}, errors.New("failed to read value")
	}

	return
}

// write writes data to data file
// first, write the size of data, second write data
func (m *MemDB) write(data []byte, offset int64) error {
	buf := make([]byte, 4)
	binary.LittleEndian.PutUint32(buf[:], uint32(len(data)))

	m.mu.Lock()
	defer m.mu.Unlock()

	n, err := m.datafd.WriteAt(buf, offset)
	if err != nil {
		return err
	}
	if n != 4 {
		return errors.New("Failed to write value size")
	}

	n, err = m.datafd.WriteAt(data, offset+4)
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

// openDataFile opens data file
func (m *MemDB) openDataFile() error {
	datafile, err := os.OpenFile(m.o.Path, os.O_APPEND|os.O_RDWR, os.ModeAppend)
	if err != nil {
		return err
	}

	m.datafd = datafile

	return nil
}

// closeDataFile closes data file
func (m *MemDB) closeDataFile() {
	m.datafd.Close()
}

// dataSize returns the size of data file
func (m *MemDB) dataSize() (size uint32, err error) {
	finfo, err := m.datafd.Stat()
	if err != nil {
		return 0, err
	}

	return uint32(finfo.Size()), nil
}

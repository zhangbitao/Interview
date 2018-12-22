package memdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"testing"
	"time"
)

func TestMemDBFromFile(t *testing.T) {
	filename := "/tmp/memfile_test.data"
	file, _ := os.Create(filename)
	file.Close()

	options := NewMemDBOptions(filename, 1000)
	memdb := NewMemDB(options)
	err := memdb.Open()
	if err != nil {
		t.Error("failed to open")
	}

	key := []byte("key_01")
	value := []byte("value_01")

	err = memdb.Set(key, value)
	if err != nil {
		t.Errorf("failed to write %s", string(key))
	}

	rvalue, err := memdb.Get(key)
	if err != nil {
		t.Errorf("failed to get %s, err: %v", string(key), err)
	}

	if string(rvalue) != string(value) {
		t.Errorf("got: %s, want: %s", rvalue, value)
	}

	defer memdb.Close()
	os.Remove(filename)
}

func TestMemDBFromIndex(t *testing.T) {
	filename := "/tmp/memfile_test2.data"
	file, _ := os.Create(filename)
	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key_%d", i))
		value := []byte(fmt.Sprintf("value_%d", i))

		buf := make([]byte, 4)
		binary.LittleEndian.PutUint32(buf[:], uint32(len(key)))
		file.Write(buf)
		file.Write(key)
		binary.LittleEndian.PutUint32(buf[:], uint32(len(value)))
		file.Write(buf)
		file.Write(value)
	}

	file.Close()

	options := NewMemDBOptions(filename, 1000)
	memdb := NewMemDB(options)
	err := memdb.Open()
	if err != nil {
		t.Error("failed to open")
	}

	time.Sleep(time.Second)

	testCases := []struct {
		key  []byte
		want []byte
	}{
		{[]byte("key_1"), []byte("value_1")},
		{[]byte("key_11"), []byte("value_11")},
		{[]byte("key_44"), []byte("value_44")},
		{[]byte("key_63"), []byte("value_63")},
		{[]byte("key_89"), []byte("value_89")},
	}

	for _, tc := range testCases {
		rvalue, err := memdb.Get(tc.key)
		if err != nil {
			t.Errorf("failed to get %s, err: %v", string(tc.key), err)
		}

		if string(rvalue) != string(tc.want) {
			t.Errorf("got: %s, want: %s", rvalue, tc.want)
		}
	}

	defer memdb.Close()
	os.Remove(filename)
}

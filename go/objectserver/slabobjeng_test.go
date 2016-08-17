package objectserver

import (
	"bytes"
	"database/sql"
	"io/ioutil"
	"os"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"
)

func newSlabFactory(deviceRoot string) *SlabObjectFactory {
	return &SlabObjectFactory{
		deviceRoot:        deviceRoot,
		hashPathPrefix:    "prefix",
		hashPathSuffix:    "suffix",
		db:                make(map[string]*sql.DB),
		slab:              make(map[string]*os.File),
		slabm:             make(map[string]*sync.Mutex),
		wholefileDirCount: 32,
		allowedSlabWaste:  128,
		slabCutoffSize:    1024 * 1024 * 2,
	}
}

func TestSlabObjectRoundtrip(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)
	swcon := newSlabFactory(deviceRoot)
	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	require.NotNil(t, swo)
	defer swo.Close()
	require.Nil(t, err)
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	require.Nil(t, swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"}))

	swo, err = swcon.New(vars, true)
	require.Nil(t, err)
	defer swo.Close()
	metadata := swo.Metadata()
	require.Equal(t, map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"}, metadata)
	buf := &bytes.Buffer{}
	_, err = swo.Copy(buf)
	require.Nil(t, err)
	require.Equal(t, "!", buf.String())
}

func TestSlabObjectMultiCopy(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := newSlabFactory(deviceRoot)
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	require.NotNil(t, swo)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, true)
	require.Nil(t, err)
	defer swo.Close()
	buf1 := &bytes.Buffer{}
	buf2 := &bytes.Buffer{}
	_, err = swo.Copy(buf1, buf2)
	require.Nil(t, err)
	require.Equal(t, "!", buf1.String())
	require.Equal(t, "!", buf2.String())
}

func TestSlabObjectDelete(t *testing.T) {
	deviceRoot, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(deviceRoot)

	vars := map[string]string{"device": "sda", "account": "a", "container": "c", "object": "o", "partition": "1"}
	swcon := newSlabFactory(deviceRoot)
	swo, err := swcon.New(vars, false)
	require.Nil(t, err)
	require.NotNil(t, swo)
	defer swo.Close()
	w, err := swo.SetData(1)
	require.Nil(t, err)
	w.Write([]byte("!"))
	swo.Commit(map[string]string{"Content-Length": "1", "Content-Type": "text/plain", "X-Timestamp": "1234567890.123456"})

	swo, err = swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	require.True(t, swo.Exists())
	err = swo.Delete(map[string]string{"X-Timestamp": "1234567891.123456"})
	require.Nil(t, err)

	swo, err = swcon.New(vars, false)
	require.Nil(t, err)
	defer swo.Close()
	require.False(t, swo.Exists())
}

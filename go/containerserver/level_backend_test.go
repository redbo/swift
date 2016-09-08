package containerserver

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLevelListObjects(t *testing.T) {
	dir, err := ioutil.TempDir("", "")
	require.Nil(t, err)
	defer os.RemoveAll(dir)
	dbfile := filepath.Join(dir, "db")
	require.Nil(t, levelCreateContainer(dbfile, "a", "c", "1473531832.13028", map[string][]string{}, 0))
	db, err := levelOpenContainer(dbfile)
	require.Nil(t, err)
	require.Nil(t, mergeItemsByName(db, []string{"a", "b", "c"}))
	records, err := db.ListObjects(10000, "", "", "", "", nil, false, 0)
	require.Nil(t, err)
	require.Equal(t, 3, len(records))
	require.Equal(t, "a", records[0].(*ObjectListingRecord).Name)
	require.Equal(t, "b", records[1].(*ObjectListingRecord).Name)
	require.Equal(t, "c", records[2].(*ObjectListingRecord).Name)
}

//  Copyright (c) 2015 Rackspace
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
//  implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

package objectserver

import (
	"bufio"
	"crypto/md5"
	"database/sql"
	"encoding/hex"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/openstack/swift/go/hummingbird"

	sqlite "github.com/mattn/go-sqlite3"
)

const (
	journalFile = "hashes.db.pending"
	indexDbFile = "hashes.db"
	pklFile     = "hashes.pkl"
)

func chexor(oldhash string, add string) string {
	addhash := md5.Sum([]byte(add))
	oh, err := hex.DecodeString(oldhash)
	if err != nil {
		panic("old hash isn't unhexable")
	}
	for i, v := range addhash {
		oh[i] ^= v
	}
	return hex.EncodeToString(oh)
}

// This is terrible and it's all my fault.
type StupidHashSlice []string

func (p StupidHashSlice) Len() int      { return len(p) }
func (p StupidHashSlice) Swap(i, j int) { p[i], p[j] = p[j], p[i] }
func (p StupidHashSlice) Less(i, j int) bool {
	dir1, file1 := filepath.Split(p[i])
	dir2, file2 := filepath.Split(p[j])
	if dir1 == dir2 {
		return file1 > file2
	} else {
		return dir1 < dir2
	}
}

type suffixHash struct {
	fileList StupidHashSlice
}

func (s *suffixHash) Step(file string) {
	s.fileList = append(s.fileList, file)
}

func (s *suffixHash) Done() string {
	sort.Sort(s.fileList)
	hsh := md5.New()
	hashed := ""
	for _, f := range s.fileList {
		hashed += filepath.Base(f)
		io.WriteString(hsh, filepath.Base(f))
	}
	return hex.EncodeToString(hsh.Sum(nil))
}

func init() {
	sql.Register("sqlite3_hashesdb", &sqlite.SQLiteDriver{
		ConnectHook: func(conn *sqlite.SQLiteConn) error {
			if err := conn.RegisterFunc("chexor", chexor, true); err != nil {
				return err
			}
			if err := conn.RegisterAggregator("suffix_hash", func() *suffixHash { return &suffixHash{} }, true); err != nil {
				return err
			}
			return nil
		},
	})
}

type PartitionIndex struct {
	db      *sql.DB
	partDir string
}

/*
func (p *PartitionIndex) printQuery(query string, args ...interface{}) {
	rows, err := p.db.Query(query, args...)
	if err != nil {
		return
	}
	defer rows.Close()
	fmt.Println("QUERY RESULTS:", query)
	for rows.Next() {
		colnames, err := rows.Columns()
		if err != nil {
			return
		}
		vals := make([]string, len(colnames))
		cols := make([]interface{}, len(colnames))
		for i := range cols {
			cols[i] = &vals[i]
		}
		rows.Scan(cols...)
		fmt.Println("   ", vals)
	}
}
*/

func (p *PartitionIndex) addRemoveFiles(adds []string, removes []string) error {
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()
	ins, err := tx.Prepare("INSERT OR IGNORE INTO filelist (filename, tombstone_ts, suffix) VALUES (?, ?, ?)")
	if err != nil {
		return err
	}
	dels, err := tx.Prepare("DELETE FROM filelist WHERE filename=?")
	if err != nil {
		return err
	}

	for _, add := range adds {
		suff, _, timestamp, ext := fileInfo(add)
		if ext == "ts" {
			ins.Exec(add, timestamp, suff)
		} else {
			ins.Exec(add, "", suff)
		}
	}
	for _, remove := range removes {
		dels.Exec(remove)
	}
	return tx.Commit()
}

func (p *PartitionIndex) expiredTombstones(reclaimAge int64) ([]string, error) {
	rows, err := p.db.Query("SELECT filename FROM filelist WHERE tombstone_ts < ?", time.Now().Unix()-reclaimAge)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ret []string
	for rows.Next() {
		var s string
		if err = rows.Scan(&s); err != nil {
			return nil, err
		}
		ret = append(ret, s)
	}
	return ret, nil
}

func (p *PartitionIndex) getHashes() (map[string]string, error) {
	hashes := map[string]string{}
	rows, err := p.db.Query("SELECT suffix, hash from hashes")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var suffix, hash string
		if err = rows.Scan(&suffix, &hash); err != nil {
			return nil, err
		}
		hashes[suffix] = hash
	}
	return hashes, nil
}

func (p *PartitionIndex) filesInSuffixes(suffixes []string) ([]string, error) {
	fileList := []string{}
	if len(suffixes) == 0 {
		return fileList, nil
	}
	sufflist := make([]interface{}, len(suffixes))
	for i := range suffixes {
		sufflist[i] = suffixes[i]
	}
	rows, err := p.db.Query("SELECT filename FROM filelist WHERE suffix in ("+strings.Repeat("?,", len(suffixes)-1)+"?)", sufflist...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	for rows.Next() {
		var fileName string
		if err = rows.Scan(&fileName); err != nil {
			return nil, err
		}
		fileList = append(fileList, fileName)
	}
	return fileList, nil
}

func (p *PartitionIndex) populateHash(suffix string) (string, error) {
	var unpop int64
	if _, err := p.db.Exec("INSERT OR IGNORE INTO hashes (suffix) VALUES (?)", suffix); err != nil {
		return "", err
	}
	if err := p.db.QueryRow("SELECT unpopulated FROM hashes WHERE suffix=?", suffix).Scan(&unpop); err != nil {
		return "", err
	}
	if unpop == 1 {
		objFiles, err := listObjFiles(p.partDir,
			func(suff string) bool { return suff == suffix })
		if err != nil {
			return "", err
		}
		if err = p.addRemoveFiles(objFiles, nil); err != nil {
			return "", err
		}
	}
	if _, err := p.db.Exec("UPDATE hashes SET unpopulated=0, hash=(SELECT suffix_hash(filename) FROM filelist WHERE filelist.suffix=hashes.suffix) WHERE suffix=?", suffix); err != nil {
		return "", err
	}
	var s string
	err := p.db.QueryRow("SELECT hash FROM hashes WHERE suffix=?", suffix).Scan(&s)
	return s, err
}

func (p *PartitionIndex) Close() error {
	return p.db.Close()
}

func OpenPartitionIndex(partDir string) (*PartitionIndex, error) {
	partDir = filepath.Clean(partDir)
	finished := false
	existed := hummingbird.Exists(filepath.Join(partDir, indexDbFile))
	db, err := sql.Open("sqlite3_hashesdb", filepath.Join(partDir, indexDbFile))
	if err != nil {
		return nil, err
	}
	defer func() {
		if !finished {
			db.Close()
		}
	}()
	// create the database if none existed
	if !existed {
		tx, err := db.Begin()
		if err != nil {
			return nil, err
		}
		defer tx.Rollback()
		for _, stmt := range []string{
			"CREATE TABLE filelist (filename TEXT PRIMARY KEY, tombstone_ts REAL, suffix TEXT)",
			"CREATE TABLE hashes (suffix TEXT PRIMARY KEY, hash TEXT DEFAULT '', nhash TEXT DEFAULT '00000000000000000000000000000000', unpopulated INTEGER DEFAULT 1)",
			`CREATE TRIGGER insert_update_nhash AFTER INSERT ON filelist
    			 BEGIN
					INSERT OR IGNORE INTO hashes (suffix) VALUES (new.suffix);
				 	UPDATE hashes SET hash='', nhash = chexor(nhash, new.filename) WHERE hash=new.suffix;
				 END`,
			`CREATE TRIGGER delete_update_nhash AFTER DELETE ON filelist
    		 	 BEGIN
					INSERT OR IGNORE INTO hashes (suffix) VALUES (old.suffix);
				 	UPDATE hashes SET hash='', nhash = chexor(nhash, old.filename) WHERE hash=old.suffix;
				 END`,
		} {
			if _, err = tx.Exec(stmt); err != nil {
				return nil, err
			}
		}
		if err = tx.Commit(); err != nil {
			return nil, err
		}
	}
	if _, err = db.Exec("PRAGMA journal_mode = WAL"); err != nil {
		return nil, err
	}
	if _, err = db.Exec("PRAGMA synchronous = NORMAL"); err != nil {
		return nil, err
	}

	p := &PartitionIndex{db: db, partDir: partDir}

	// apply any journaled changes
	dbJournal := filepath.Join(partDir, journalFile)
	if journal, err := os.OpenFile(dbJournal, os.O_RDWR, 0660); err == nil {
		defer journal.Close()
		if err := hummingbird.LockFile(journal, 10); err != nil {
			return nil, err
		}
		adds := []string{}
		dels := []string{}
		scanner := bufio.NewScanner(journal)
		for scanner.Scan() {
			recs := strings.Split(scanner.Text(), ":")
			if len(recs) == 2 {
				if recs[0] == "add" {
					adds = append(adds, recs[1])
				} else if recs[0] == "del" {
					dels = append(dels, recs[1])
				}
			}
		}
		if len(adds) > 0 || len(dels) > 0 {
			if err := p.addRemoveFiles(adds, dels); err != nil {
				return nil, err
			}
		}
		journal.Truncate(0)
	}

	finished = true
	return p, nil
}

func fileInfo(path string) (suffix string, objHash string, timestamp string, ext string) {
	parts := strings.Split(path, string(filepath.Separator))
	basename := parts[len(parts)-1]
	ext = filepath.Ext(basename)[1:]
	timestamp = basename[:len(basename)-len(ext)-1]
	objHash = parts[len(parts)-2]
	suffix = parts[len(parts)-3]
	return
}

func appendJournal(op string, path string) error {
	partDir := filepath.Dir(filepath.Dir(filepath.Dir(path)))
	if !hummingbird.Exists(partDir) {
		return nil
	}
	pathInPartition, err := filepath.Rel(path, partDir)
	if err != nil {
		return err
	}
	dbJournal := filepath.Join(partDir, journalFile)
	file, err := os.OpenFile(dbJournal, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0660)
	if err != nil {
		return err
	}
	defer file.Close()
	if err := hummingbird.LockFile(file, 10); err != nil {
		return err
	}
	fmt.Fprintf(file, "%s:%s\n", op, pathInPartition)
	return file.Sync()
}

func IndexRemoveFile(path string) error {
	return appendJournal("del", path)
}

func IndexAddFile(path string) error {
	return appendJournal("add", path)
}

func IndexRemoveAll(path string) error {
	return nil
}

func GetHashes(driveRoot string, device string, partition string, recalculate []string, logger hummingbird.LoggingContext) (map[string]string, error) {
	partDir := filepath.Join(driveRoot, device, "objects", partition)
	hashes := make(map[string]string, 4096)

	// read dir occasionally for any missed suffixes
	if rand.Int31n(10) == 0 {
		suffs, err := hummingbird.ReadDirNames(partDir)
		if err != nil {
			return nil, err
		}
		for _, suffName := range suffs {
			if len(suffName) == 3 {
				hashes[suffName] = ""
			}
		}
	}

	// override any hashes with values from hashes.pkl
	if data, err := ioutil.ReadFile(filepath.Join(partDir, pklFile)); err == nil {
		if v, err := hummingbird.PickleLoads(data); err == nil {
			if ph, ok := v.(map[interface{}]interface{}); ok {
				for key, value := range ph {
					if keys, ok := key.(string); ok {
						if vals, ok := value.(string); ok {
							hashes[keys] = vals
						} else {
							hashes[keys] = ""
						}
					}
				}
			}
		}
	}

	// load in hashes from the database
	p, err := OpenPartitionIndex(partDir)
	if err != nil {
		return nil, err
	}
	defer p.Close()
	dbHashes, err := p.getHashes()
	if err != nil {
		return nil, err
	}
	for key, value := range dbHashes {
		hashes[key] = value
	}

	for _, recalc := range recalculate {
		hashes[recalc] = ""
	}

	// populate any missing hashes
	for key, value := range hashes {
		if value == "" {
			if hashes[key], err = p.populateHash(key); err != nil {
				return nil, err
			}
		}
	}

	// write out a hashes.pkl in case of a dystopian future where we have to roll back
	hummingbird.WriteFileAtomic(filepath.Join(partDir, pklFile), hummingbird.PickleDumps(hashes), 0600)
	return hashes, nil
}

// The goal is for this to go away, in favor of IndexAddFile and friends
func InvalidateHash(hashDir string) error {
	suffDir, _ := filepath.Split(hashDir)
	partDir, suffix := filepath.Split(suffDir)
	p, err := OpenPartitionIndex(partDir)
	if err != nil {
		return err
	}
	defer p.Close()
	tx, err := p.db.Begin()
	if err != nil {
		return err
	}
	if _, err := tx.Exec("DELETE FROM filelist WHERE suffix=?", suffix); err != nil {
		return err
	}
	if _, err := tx.Exec("UPDATE hashes SET unpopulated=1, hash='', nhash='00000000000000000000000000000000' WHERE suffix=?", suffix); err != nil {
		return err
	}
	return tx.Commit()
}

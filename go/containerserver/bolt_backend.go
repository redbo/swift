package containerserver

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/boltdb/bolt"
	"github.com/openstack/swift/go/hummingbird"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type boltContainer struct {
	*bolt.DB
	connectLock   sync.Mutex
	containerFile string
	infoCache     atomic.Value
	ringhash      string
}

var _ ReplicableContainer = &boltContainer{}

func (db *boltContainer) connect() error {
	db.connectLock.Lock()
	defer db.connectLock.Unlock()
	if db.DB != nil {
		return nil
	}
	var err error
	db.DB, err = bolt.Open(db.containerFile, 0644, &bolt.Options{Timeout: 10 * time.Second})
	return err
}

func (db *boltContainer) GetInfo() (*ContainerInfo, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	if err := db.flush(); err != nil {
		return nil, err
	}
	if info, ok := db.infoCache.Load().(*ContainerInfo); ok && !info.invalid && time.Since(info.updated) < infoCacheTimeout {
		return info, nil
	}
	ci := &ContainerInfo{updated: time.Now()}
	err := db.View(func(tx *bolt.Tx) error {
		var err error
		b := tx.Bucket([]byte("info"))
		return b.ForEach(func(k, v []byte) error {
			switch string(k) {
			case "account":
				ci.Account = string(v)
			case "container":
				ci.Container = string(v)
			case "put_timestamp":
				ci.PutTimestamp = string(v)
			case "delete_timestamp":
				ci.DeleteTimestamp = string(v)
			case "status_changed_at":
				ci.StatusChangedAt = string(v)
			case "hash":
				ci.Hash = string(v)
			case "id":
				ci.ID = string(v)
			case "max_rowid":
				if ci.MaxRow, err = strconv.ParseInt(string(v), 10, 64); err != nil {
					return fmt.Errorf("Parsing max row: %v", err)
				}
			case "object_count":
				if ci.ObjectCount, err = strconv.ParseInt(string(v), 10, 64); err != nil {
					return fmt.Errorf("Parsing object count: %v", err)
				}
			case "bytes_used":
				if ci.BytesUsed, err = strconv.ParseInt(string(v), 10, 64); err != nil {
					return fmt.Errorf("Parsing bytes used: %v", err)
				}
			case "storage_policy_index":
				if pi, err := strconv.ParseInt(string(v), 10, 64); err != nil {
					return fmt.Errorf("Parsing bytes used: %v", err)
				} else {
					ci.StoragePolicyIndex = int(pi)
				}
			case "metadata":
				ci.RawMetadata = string(v)
				if err := json.Unmarshal(v, &ci.Metadata); err != nil {
					return err
				}
			}
			return nil
		})
	})
	if err != nil {
		return nil, fmt.Errorf("Error getting info: %v", err)
	}
	db.infoCache.Store(ci)
	return ci, nil
}

func (db *boltContainer) invalidateCache() {
	db.infoCache.Store(&ContainerInfo{invalid: true})
}

func (db *boltContainer) IsDeleted() (bool, error) {
	if info, err := db.GetInfo(); err != nil {
		return false, err
	} else {
		return info.DeleteTimestamp > info.PutTimestamp, nil
	}
}

func (db *boltContainer) Delete(timestamp string) error {
	if err := db.connect(); err != nil {
		return err
	}
	defer db.invalidateCache()
	return db.Update(func(tx *bolt.Tx) error {
		// TODO: this should only happen if the timestamp is newer
		b := tx.Bucket([]byte("info"))
		if err := b.Put([]byte("delete_timestamp"), []byte(timestamp)); err != nil {
			return err
		}
		metab := b.Get([]byte("metadata"))
		if metab != nil {
			var metadata map[string][]string
			if err := json.Unmarshal(metab, &metadata); err != nil {
				return err
			}
			for key, value := range metadata {
				if value[0] != "" {
					metadata[key] = []string{"", timestamp}
				}
			}
			metab, err := json.Marshal(metadata)
			if err != nil {
				return err
			}
			return b.Put([]byte("metadata"), metab)
		}
		return nil
	})
}

func (db *boltContainer) ListObjects(limit int, marker string, endMarker string, prefix string, delimiter string, path *string, reverse bool, storagePolicyIndex int) ([]interface{}, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	if path != nil {
		if *path != "" {
			p := strings.TrimRight(*path, "/") + "/"
			path = &p
		}
		delimiter = "/"
		prefix = *path
	}

	results := make([]interface{}, 0, limit)

	err := db.View(func(tx *bolt.Tx) error {
		var point string
		gotResults := true
		c := tx.Bucket([]byte("objects")).Cursor()
		startpoint := func(start string) (k, v []byte) {
			return c.Seek([]byte(start))
		}
		step := c.Next
		if reverse {
			startpoint = func(start string) (k, v []byte) {
				k, v = c.Seek([]byte(start))
				if bytes.Compare(k, []byte(start)) != 0 {
					return c.Prev()
				}
				return k, v
			}
			step = c.Prev
		}

		for len(results) < limit && gotResults {
			start := "0/"
			if point != "" {
				start += point
			} else if marker != "" {
				if reverse {
					start += marker[:len(marker)-1] + string(marker[len(marker)-1]-1)
				} else {
					start += marker + "\xFF"
				}
			} else if prefix != "" {
				if reverse {
					start += prefix + "\xFF"
				} else {
					start += prefix
				}
			} else if reverse {
				start += "\xFF"
			}

			gotResults = false
			for k, v := startpoint(start); k != nil && bytes.HasPrefix(k, []byte{'0', '/'}) && len(results) < limit; k, v = step() {
				if endMarker != "" && !reverse && string(k) >= "0/"+endMarker {
					return nil
				} else if endMarker != "" && reverse && string(k) <= "0/"+endMarker {
					return nil
				} else if prefix != "" && reverse && string(k) < "0/"+prefix {
					return nil
				} else if prefix != "" && !reverse && string(k) >= "0/"+prefix+"\xFF" {
					return nil
				}
				gotResults = true
				record := &ObjectListingRecord{}
				if err := msgpack.Unmarshal(v, record); err != nil {
					return err
				}
				if reverse {
					point = record.Name[:len(record.Name)-1] + string(record.Name[len(record.Name)-1]-1)
				} else {
					point = record.Name + "\xFF"
				}
				if delimiter != "" {
					if path != nil && record.Name == *path {
						continue
					}
					end := indexAfter(record.Name, delimiter, len(prefix))
					if end >= 0 && (path == nil || len(record.Name) > end+1) {
						dirName := record.Name[:end] + delimiter
						if reverse {
							point = record.Name[:end+len(delimiter)]
						} else {
							point = dirName + "\xFF"
						}
						if path == nil && dirName != marker {
							results = append(results, &SubdirListingRecord{Name2: dirName, Name: dirName})
						}
						break
					}
				}
				updateRecord(record)
				results = append(results, record)
			}
			if delimiter == "" && path == nil {
				break
			}
		}
		return nil
	})
	return results, err
}

func (db *boltContainer) GetMetadata() (map[string]string, error) {
	info, err := db.GetInfo()
	if err != nil {
		return nil, err
	}
	metadata := make(map[string]string)
	for key, value := range info.Metadata {
		if value[0] != "" {
			metadata[key] = value[0]
		}
	}
	return metadata, nil
}

func (db *boltContainer) mergeMetas(a map[string][]string, b map[string][]string) ([]byte, error) {
	newMeta := map[string][]string{}
	for k, v := range a {
		newMeta[k] = v
	}
	for k, v := range b {
		if existing, ok := a[k]; ok {
			if existing[1] < v[1] {
				newMeta[k] = v
			}
		} else {
			newMeta[k] = v
		}
	}
	metaSize := 0
	metaCount := 0
	for k, v := range newMeta {
		if v[0] != "" && strings.HasPrefix(strings.ToLower(k), "x-container-meta-") {
			metaSize += len(k) - 17
			metaSize += len(v[0])
			metaCount += 1
		}
	}
	if metaCount > maxMetaCount || metaSize > maxMetaOverallSize {
		return nil, ErrorInvalidMetadata
	}
	serMeta, err := json.Marshal(newMeta)
	if err != nil {
		return nil, err
	}
	return serMeta, nil
}

func (db *boltContainer) UpdateMetadata(updates map[string][]string) error {
	if err := db.connect(); err != nil {
		return err
	}
	defer db.invalidateCache()
	if len(updates) == 0 {
		return nil
	}
	var existingMetadata map[string][]string
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("info"))
		metadata := b.Get([]byte("metadata"))
		if metadata == nil {
			existingMetadata = map[string][]string{}
		} else if err := json.Unmarshal(metadata, &existingMetadata); err != nil {
			return err
		}
		metab, err := db.mergeMetas(existingMetadata, updates)
		if err != nil {
			return err
		}
		return b.Put([]byte("metadata"), metab)
	})
}

func (db *boltContainer) MergeItems(records []*ObjectRecord, remoteID string) error {
	if err := db.connect(); err != nil {
		return err
	}
	type RecordID struct {
		policy int
		name   string
	}
	names := make([]string, len(records))
	existing := make(map[RecordID]ObjectRecord)
	toAdd := make(map[RecordID]*ObjectRecord)
	for i, record := range records {
		names[i] = record.Name
	}
	defer db.invalidateCache()
	return db.Update(func(tx *bolt.Tx) error {
		incomingSyncs := tx.Bucket([]byte("incomingsyncs"))
		objects := tx.Bucket([]byte("objects"))
		info := tx.Bucket([]byte("info"))
		rowids := tx.Bucket([]byte("rowids"))

		ocb := info.Get([]byte("object_count"))
		bub := info.Get([]byte("bytes_used"))
		hsb := info.Get([]byte("hash"))
		if ocb == nil || bub == nil || hsb == nil {
			return fmt.Errorf("Error getting info")
		}
		objectCount, err := strconv.ParseInt(string(ocb), 10, 64)
		if err != nil {
			return err
		}
		bytesUsed, err := strconv.ParseInt(string(bub), 10, 64)
		if err != nil {
			return err
		}
		hash := string(hsb)

		for _, name := range names {
			var obj ObjectRecord
			objb := objects.Get([]byte("1/" + name))
			if objb == nil {
				objb = objects.Get([]byte("0/" + name))
			}
			if objb == nil {
				continue
			}
			if err := msgpack.Unmarshal(objb, &obj); err != nil {
				return err
			}
			existing[RecordID{policy: obj.StoragePolicyIndex, name: obj.Name}] = obj
		}
		for _, record := range records {
			rid := RecordID{policy: record.StoragePolicyIndex, name: record.Name}
			if current, inExisting := existing[rid]; !inExisting {
				toAdd[rid] = record
			} else if current.CreatedAt < record.CreatedAt {
				if err := rowids.Delete([]byte(fmt.Sprintf("%019d", current.Rowid))); err != nil {
					return err
				}
				if current.Deleted != 1 {
					objectCount -= 1
					bytesUsed -= current.Size
				}
				if alreadyIn, ok := toAdd[rid]; !ok || record.CreatedAt > alreadyIn.CreatedAt {
					toAdd[rid] = record
				}
			}
		}
		var maxIncomingRowid, maxLocalRowid int64
		for _, record := range toAdd {
			if record.Rowid > maxIncomingRowid {
				maxIncomingRowid = record.Rowid
			}
			rowid, err := objects.NextSequence()
			if err != nil {
				return err
			}
			record.Rowid = int64(rowid)
			maxLocalRowid = record.Rowid
			objb, err := msgpack.Marshal(record)
			if err != nil {
				return err
			}
			recKey := []byte(fmt.Sprintf("%d/%s", record.Deleted, record.Name))
			if err := objects.Put(recKey, objb); err != nil {
				return err
			}
			rowidKey := []byte(fmt.Sprintf("%019d", record.Rowid))
			if err := rowids.Put(rowidKey, recKey); err != nil {
				return err
			}
			hash = chexor(hash, record.Name, record.CreatedAt)
			if record.Deleted != 1 {
				objectCount += 1
				bytesUsed += record.Size
			}
		}
		if remoteID != "" {
			if err := incomingSyncs.Put([]byte(remoteID), []byte(strconv.FormatInt(maxIncomingRowid, 10))); err != nil {
				return err
			}
		}
		if err := info.Put([]byte("object_count"), []byte(strconv.FormatInt(objectCount, 10))); err != nil {
			return err
		}
		if err := info.Put([]byte("bytes_used"), []byte(strconv.FormatInt(bytesUsed, 10))); err != nil {
			return err
		}
		if err := info.Put([]byte("max_rowid"), []byte(strconv.FormatInt(maxLocalRowid, 10))); err != nil {
			return err
		}
		if err := info.Put([]byte("hash"), []byte(hash)); err != nil {
			return err
		}
		return nil
	})
}

func (db *boltContainer) ItemsSince(start int64, count int) ([]*ObjectRecord, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	results := []*ObjectRecord{}
	err := db.View(func(tx *bolt.Tx) error {
		db := tx.Bucket([]byte("rowids")).Cursor()
		objects := tx.Bucket([]byte("objects"))
		startKey := []byte(fmt.Sprintf("%019d!", start))
		for k, v := db.Seek(startKey); k != nil && len(results) < count; k, v = db.Next() {
			objb := objects.Get(v)
			if objb == nil {
				continue
			}
			var obj ObjectRecord
			if err := msgpack.Unmarshal(objb, &obj); err != nil {
				return err
			}
			results = append(results, &obj)
		}
		return nil
	})
	return results, err
}

func (db *boltContainer) MergeSyncTable(records []*SyncRecord) error {
	if err := db.connect(); err != nil {
		return err
	}
	return db.Update(func(tx *bolt.Tx) error {
		incomingSyncs := tx.Bucket([]byte("incomingsyncs"))
		for _, rec := range records {
			val := incomingSyncs.Get([]byte(rec.RemoteID))
			if val != nil {
				current, err := strconv.ParseInt(string(val), 10, 64)
				if err != nil {
					return err
				}
				if current >= rec.SyncPoint {
					continue
				}
			}
			if err := incomingSyncs.Put([]byte(rec.RemoteID), []byte(strconv.FormatInt(rec.SyncPoint, 10))); err != nil {
				return err
			}
		}
		return nil
	})
}

func (db *boltContainer) SyncTable() ([]*SyncRecord, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	results := []*SyncRecord{}
	err := db.View(func(tx *bolt.Tx) error {
		info := tx.Bucket([]byte("info"))
		b := tx.Bucket([]byte("incomingsyncs"))
		b.ForEach(func(k, v []byte) error {
			syncPoint, err := strconv.ParseInt(string(v), 10, 64)
			if err != nil {
				return err
			}
			results = append(results, &SyncRecord{RemoteID: string(k), SyncPoint: syncPoint})
			return nil
		})
		id := info.Get([]byte("id"))
		maxRowb := info.Get([]byte("max_rowid"))
		if id != nil && maxRowb != nil {
			maxRow, err := strconv.ParseInt(string(maxRowb), 10, 64)
			if err != nil {
				return err
			}
			results = append(results, &SyncRecord{RemoteID: string(id), SyncPoint: maxRow})
		}
		return nil
	})
	return results, err
}

func (db *boltContainer) SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*ContainerInfo, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	var localPoint int64
	err := db.Update(func(tx *bolt.Tx) error {
		var err error
		incomingSyncs := tx.Bucket([]byte("incomingsyncs"))
		info := tx.Bucket([]byte("info"))
		hsb := info.Get([]byte("hash"))
		ptb := info.Get([]byte("put_timestamp"))
		dtb := info.Get([]byte("delete_timestamp"))
		cab := info.Get([]byte("created_at"))
		metb := info.Get([]byte("metadata"))
		if hsb == nil || metb == nil || ptb == nil || dtb == nil || cab == nil {
			return fmt.Errorf("Getting info")
		}
		var lm, rm map[string][]string
		if err := json.Unmarshal([]byte(metadata), &rm); err != nil {
			return err
		}
		if err := json.Unmarshal(metb, &lm); err != nil {
			return err
		}
		if metb, err = db.mergeMetas(lm, rm); err != nil {
			return err
		}
		if err := info.Put([]byte("metadata"), metb); err != nil {
			return err
		}
		if createdAt < string(cab) {
			if err := info.Put([]byte("created_at"), []byte(createdAt)); err != nil {
				return err
			}
		}
		if putTimestamp > string(ptb) {
			if err := info.Put([]byte("put_timestamp"), []byte(putTimestamp)); err != nil {
				return err
			}
		}
		if deleteTimestamp > string(dtb) {
			if err := info.Put([]byte("delete_timestamp"), []byte(deleteTimestamp)); err != nil {
				return err
			}
		}
		if string(hsb) == hash {
			if err := incomingSyncs.Put([]byte(id), []byte(strconv.FormatInt(maxRow, 10))); err != nil {
				return err
			}
		}
		pb := incomingSyncs.Get([]byte(id))
		if pb == nil {
			localPoint = -1
		} else if localPoint, err = strconv.ParseInt(string(pb), 10, 64); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	db.invalidateCache()
	info, err := db.GetInfo()
	if err != nil {
		return nil, err
	}
	info.Point = localPoint
	return info, nil
}

func (db *boltContainer) ID() string {
	return db.ringhash
}

func (db *boltContainer) RingHash() string {
	return db.ringhash
}

func (db *boltContainer) NewID() error {
	if err := db.connect(); err != nil {
		return err
	}
	defer db.invalidateCache()
	return db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("info"))
		return b.Put([]byte("id"), []byte(hummingbird.UUID()))
	})
}

func (db *boltContainer) OpenDatabaseFile() (*os.File, func(), error) {
	tx, err := db.Begin(true)
	if err != nil {
		return nil, nil, err
	}
	fp, err := os.Open(db.containerFile)
	if err != nil {
		tx.Rollback()
		return nil, nil, fmt.Errorf("Error opening %s: %v", db.containerFile, err)
	}
	cleanup := func() {
		fp.Close()
		tx.Rollback()
	}
	return fp, cleanup, nil
}

func (db *boltContainer) CleanupTombstones(reclaimAge int64) error {
	if err := db.connect(); err != nil {
		return err
	}
	now := float64(time.Now().UnixNano()) / 1000000000.0
	reclaimTimestamp := hummingbird.CanonicalTimestamp(now - float64(reclaimAge))
	prefix := []byte("1/")
	return db.Update(func(tx *bolt.Tx) error {
		info := tx.Bucket([]byte("info"))
		hsb := info.Get([]byte("hash"))
		if hsb == nil {
			return errors.New("Invalid hash")
		}
		hash := string(hsb)

		objects := tx.Bucket([]byte("objects"))
		rowids := tx.Bucket([]byte("rowids"))
		c := objects.Cursor()
		for k, v := c.Seek(prefix); bytes.HasPrefix(k, prefix); k, v = c.Next() {
			objb := objects.Get(v)
			if objb == nil {
				continue
			}
			var obj ObjectRecord
			if err := msgpack.Unmarshal(objb, &obj); err != nil {
				return err
			}
			if obj.CreatedAt < reclaimTimestamp {
				if err := objects.Delete(k); err != nil {
					return err
				}
				if err := rowids.Delete([]byte(fmt.Sprintf("%019d", obj.Rowid))); err != nil {
					return err
				}
			}
			hash = chexor(hash, obj.Name, obj.CreatedAt)
		}
		return info.Put([]byte("hash"), []byte(hash))
	})
}

func (db *boltContainer) flushAlreadyLocked() error {
	if err := db.connect(); err != nil {
		return err
	}
	if stat, err := os.Stat(db.containerFile + ".pending"); err != nil || stat.Size() == 0 {
		return nil
	}
	contents, err := ioutil.ReadFile(db.containerFile + ".pending")
	if err != nil || len(contents) == 0 {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}
	var records []*ObjectRecord
	for _, base64ed := range strings.Split(string(contents), ":") {
		if len(base64ed) < 1 {
			continue
		}
		pickled, err := base64.StdEncoding.DecodeString(base64ed)
		if err != nil {
			continue
		}
		r, err := hummingbird.PickleLoads(pickled)
		if err != nil {
			continue
		}
		record, ok := r.([]interface{})
		if !ok || len(record) < 7 {
			return fmt.Errorf("Invalid commit pending record")
		}
		casts := make([]bool, 7)
		var deleted, policy int64
		rec := &ObjectRecord{}
		rec.Name, casts[0] = record[0].(string)
		rec.CreatedAt, casts[1] = record[1].(string)
		rec.Size, casts[2] = record[2].(int64)
		rec.ContentType, casts[3] = record[3].(string)
		rec.ETag, casts[4] = record[4].(string)
		deleted, casts[5] = record[5].(int64)
		policy, casts[6] = record[6].(int64)
		rec.Deleted = int(deleted)
		rec.StoragePolicyIndex = int(policy)
		for i := 0; i < 7; i++ {
			if !casts[i] {
				return fmt.Errorf("Invalid commit pending record")
			}
		}
		records = append(records, rec)
	}
	err = db.MergeItems(records, "")
	if err == nil {
		err = os.Truncate(db.containerFile+".pending", 0)
	}
	return err
}

func (db *boltContainer) flush() error {
	lock, err := hummingbird.LockPath(filepath.Dir(db.containerFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	return db.flushAlreadyLocked()
}

func (db *boltContainer) addObject(name string, timestamp string, size int64, contentType string, etag string, deleted int, storagePolicyIndex int) error {
	lock, err := hummingbird.LockPath(filepath.Dir(db.containerFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	tuple := []interface{}{name, timestamp, size, contentType, etag, deleted, storagePolicyIndex}
	file, err := os.OpenFile(db.containerFile+".pending", os.O_RDWR|os.O_APPEND|os.O_CREATE, 0644)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err := file.WriteString(":" + base64.StdEncoding.EncodeToString(hummingbird.PickleDumps(tuple))); err != nil {
		return err
	}
	if info, err := file.Stat(); err == nil && info.Size() > pendingCap {
		db.flushAlreadyLocked()
	}
	return nil
}

// PutObject adds an object to the container, by way of pending file.
func (db *boltContainer) PutObject(name string, timestamp string, size int64, contentType string, etag string, storagePolicyIndex int) error {
	return db.addObject(name, timestamp, size, contentType, etag, 0, storagePolicyIndex)
}

// DeleteObject removes an object from the container, by way of pending file.
func (db *boltContainer) DeleteObject(name string, timestamp string, storagePolicyIndex int) error {
	return db.addObject(name, timestamp, 0, "", "", 1, storagePolicyIndex)
}

// CheckSyncLink does nothing for now, as the bolt container does not yet support container sync.
func (db *boltContainer) CheckSyncLink() error {
	return nil
}

func boltCreateExistingContainer(db Container, putTimestamp string, newMetadata map[string][]string, policyIndex int) (bool, error) {
	cdb, ok := db.(*boltContainer)
	if !ok {
		return false, errors.New("Unable to work with non-boltContainer")
	}
	created := false
	defer cdb.invalidateCache()
	err := cdb.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte("info"))
		bDeleteTimestamp := b.Get([]byte("delete_timestamp"))
		bPutTimestamp := b.Get([]byte("put_timestamp"))
		bMetadata := b.Get([]byte("metadata"))
		bPolicyIndex := b.Get([]byte("storage_policy_index"))
		if bDeleteTimestamp == nil || bPutTimestamp == nil || bMetadata == nil {
			return fmt.Errorf("Error getting info")
		}
		cDeleteTimestamp := string(bDeleteTimestamp)
		cPutTimestamp := string(bPutTimestamp)
		cMetadata := string(bMetadata)
		cPolicyIndex, err := strconv.ParseInt(string(bPolicyIndex), 10, 64)
		if err != nil {
			return err
		}
		if policyIndex < 0 {
			policyIndex = int(cPolicyIndex)
		}
		if cDeleteTimestamp <= cPutTimestamp && int(cPolicyIndex) != policyIndex {
			return ErrorPolicyConflict
		}
		var existingMetadata map[string][]string
		if cMetadata == "" {
			existingMetadata = make(map[string][]string)
		} else if err := json.Unmarshal([]byte(cMetadata), &existingMetadata); err != nil {
			return err
		}
		metab, err := cdb.mergeMetas(existingMetadata, newMetadata)
		if err != nil {
			return err
		}
		if err := b.Put([]byte("put_timestamp"), []byte(putTimestamp)); err != nil {
			return err
		}
		if err := b.Put([]byte("storage_policy_index"), []byte(strconv.FormatInt(int64(policyIndex), 10))); err != nil {
			return err
		}
		if err := b.Put([]byte("metadata"), metab); err != nil {
			return err
		}
		created = (cDeleteTimestamp > cPutTimestamp && putTimestamp > cDeleteTimestamp)
		return nil
	})
	return created, err
}

func boltCreateContainer(containerFile string, account string, container string, putTimestamp string, metadata map[string][]string, policyIndex int) error {
	hashDir := filepath.Dir(containerFile)
	if err := os.MkdirAll(hashDir, 0755); err != nil {
		return err
	}
	tfp, err := ioutil.TempFile(hashDir, ".newdb")
	if err != nil {
		return err
	}
	if err := tfp.Chmod(0644); err != nil {
		return err
	}
	defer tfp.Close()
	tempFile := tfp.Name()
	db, err := bolt.Open(tempFile, 0644, &bolt.Options{Timeout: 2 * time.Second})
	if err != nil {
		return err
	}
	defer db.Close()
	metaser, err := json.Marshal(metadata)
	if err != nil {
		return err
	}
	infoVals := map[string]string{
		"account":              account,
		"container":            container,
		"created_at":           hummingbird.GetTimestamp(),
		"id":                   hummingbird.UUID(),
		"put_timestamp":        putTimestamp,
		"delete_timestamp":     "",
		"status_changed_at":    putTimestamp,
		"storage_policy_index": strconv.FormatInt(int64(policyIndex), 10),
		"metadata":             string(metaser),
		"max_rowid":            "-1",
		"object_count":         "0",
		"bytes_used":           "0",
		"hash":                 "00000000000000000000000000000000",
	}
	err = db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists([]byte("incomingsyncs")); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("objects")); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		if _, err := tx.CreateBucketIfNotExists([]byte("rowids")); err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		b, err := tx.CreateBucketIfNotExists([]byte("info"))
		if err != nil {
			return fmt.Errorf("create bucket: %s", err)
		}
		for k, v := range infoVals {
			if err := b.Put([]byte(k), []byte(v)); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		return err
	}
	return os.Rename(tempFile, containerFile)
}

func boltOpenContainer(containerFile string) (*boltContainer, error) {
	if _, err := os.Stat(containerFile); os.IsNotExist(err) {
		return nil, errors.New("Does not exist.")
	}
	ringhash := filepath.Base(filepath.Dir(containerFile))
	db := &boltContainer{containerFile: containerFile, ringhash: ringhash}
	return db, nil
}

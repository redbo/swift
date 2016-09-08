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

	"github.com/openstack/swift/go/hummingbird"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/util"
	"gopkg.in/vmihailenco/msgpack.v2"
)

type levelContainer struct {
	*leveldb.DB
	connectLock   sync.Mutex
	containerFile string
	infoCache     atomic.Value
	ringhash      string
}

var _ ReplicableContainer = &levelContainer{}

func (db *levelContainer) Update(f func(*leveldb.Transaction) error) error {
	tx, err := db.DB.OpenTransaction()
	if err != nil {
		return err
	}
	if err := f(tx); err != nil {
		tx.Discard()
		return err
	}
	return tx.Commit()
}

func (db *levelContainer) connect() error {
	db.connectLock.Lock()
	defer db.connectLock.Unlock()
	if db.DB != nil {
		return nil
	}
	var err error
	db.DB, err = leveldb.OpenFile(db.containerFile, nil)
	return err
}

func (db *levelContainer) GetInfo() (*ContainerInfo, error) {
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
	iter := db.NewIterator(util.BytesPrefix([]byte("info/")), nil)
	defer iter.Release()
	var err error
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		if k == nil || v == nil {
			break
		}
		switch string(k) {
		case "info/account":
			ci.Account = string(v)
		case "info/container":
			ci.Container = string(v)
		case "info/put_timestamp":
			ci.PutTimestamp = string(v)
		case "info/delete_timestamp":
			ci.DeleteTimestamp = string(v)
		case "info/status_changed_at":
			ci.StatusChangedAt = string(v)
		case "info/hash":
			ci.Hash = string(v)
		case "info/id":
			ci.ID = string(v)
		case "info/max_rowid":
			if ci.MaxRow, err = strconv.ParseInt(string(v), 10, 64); err != nil {
				return nil, fmt.Errorf("Parsing max row: %v", err)
			}
		case "info/object_count":
			if ci.ObjectCount, err = strconv.ParseInt(string(v), 10, 64); err != nil {
				return nil, fmt.Errorf("Parsing object count: %v", err)
			}
		case "info/bytes_used":
			if ci.BytesUsed, err = strconv.ParseInt(string(v), 10, 64); err != nil {
				return nil, fmt.Errorf("Parsing bytes used: %v", err)
			}
		case "info/storage_policy_index":
			if pi, err := strconv.ParseInt(string(v), 10, 64); err != nil {
				return nil, fmt.Errorf("Parsing bytes used: %v", err)
			} else {
				ci.StoragePolicyIndex = int(pi)
			}
		case "info/metadata":
			ci.RawMetadata = string(v)
			if err := json.Unmarshal(v, &ci.Metadata); err != nil {
				return nil, err
			}
		}
	}
	if iter.Error() != nil {
		return nil, fmt.Errorf("Error getting info: %v", err)
	}
	db.infoCache.Store(ci)
	return ci, nil
}

func (db *levelContainer) invalidateCache() {
	db.infoCache.Store(&ContainerInfo{invalid: true})
}

func (db *levelContainer) IsDeleted() (bool, error) {
	if info, err := db.GetInfo(); err != nil {
		return false, err
	} else {
		return info.DeleteTimestamp > info.PutTimestamp, nil
	}
}

func (db *levelContainer) Delete(timestamp string) error {
	if err := db.connect(); err != nil {
		return err
	}
	defer db.invalidateCache()
	return db.Update(func(tx *leveldb.Transaction) error {
		current, err := tx.Get([]byte("info/delete_timestamp"), nil)
		if err != nil {
			return err
		}
		if timestamp > string(current) {
			tx.Put([]byte("info/delete_timestamp"), []byte(timestamp), nil)
			metab, err := tx.Get([]byte("info/metadata"), nil)
			if err != nil {
				return err
			}
			var metadata map[string][]string
			if err := json.Unmarshal(metab, &metadata); err != nil {
				return err
			}
			for key, value := range metadata {
				if value[0] != "" && value[1] < timestamp {
					metadata[key] = []string{"", timestamp}
				}
			}
			metab, err = json.Marshal(metadata)
			if err != nil {
				return err
			}
			return tx.Put([]byte("info/metadata"), metab, nil)
		}
		return nil
	})
}

func (db *levelContainer) ListObjects(limit int, marker string, endMarker string, prefix string, delimiter string, path *string, reverse bool, storagePolicyIndex int) ([]interface{}, error) {
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

	var point string
	gotResults := true
	iter := db.NewIterator(nil, nil)
	defer iter.Release()
	startpoint := func(start string) (k, v []byte) {
		iter.Seek([]byte(start))
		return iter.Key(), iter.Value()
	}
	step := func() (k, v []byte) {
		iter.Next()
		return iter.Key(), iter.Value()
	}
	if reverse {
		startpoint = func(start string) (k, v []byte) {
			iter.Seek([]byte(start))
			k = iter.Key()
			v = iter.Value()
			if k == nil || v == nil || bytes.Compare(k, []byte(start)) != 0 {
				iter.Prev()
				return iter.Key(), iter.Value()
			}
			return k, v
		}
		step = func() (k, v []byte) {
			iter.Prev()
			return iter.Key(), iter.Value()
		}
	}

	for len(results) < limit && gotResults {
		start := "objects/0/"
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
		for k, v := startpoint(start); k != nil && bytes.HasPrefix(k, []byte{'o', 'b', 'j', 'e', 'c', 't', 's', '/', '0'}) && len(results) < limit; k, v = step() {
			if endMarker != "" && !reverse && string(k) >= "objects/0/"+endMarker {
				break
			} else if endMarker != "" && reverse && string(k) <= "objects/0/"+endMarker {
				break
			} else if prefix != "" && reverse && string(k) < "objects/0/"+prefix {
				break
			} else if prefix != "" && !reverse && string(k) >= "objects/0/"+prefix+"\xFF" {
				break
			}
			gotResults = true
			record := &ObjectListingRecord{}
			if err := msgpack.Unmarshal(v, record); err != nil {
				return nil, err
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
	return results, nil
}

func (db *levelContainer) GetMetadata() (map[string]string, error) {
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

func (db *levelContainer) mergeMetas(a map[string][]string, b map[string][]string) ([]byte, error) {
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

func (db *levelContainer) UpdateMetadata(updates map[string][]string) error {
	if err := db.connect(); err != nil {
		return err
	}
	defer db.invalidateCache()
	if len(updates) == 0 {
		return nil
	}
	var existingMetadata map[string][]string
	return db.Update(func(tx *leveldb.Transaction) error {
		metadata, err := tx.Get([]byte("info/metadata"), nil)
		if err != nil {
			existingMetadata = map[string][]string{}
		} else if err := json.Unmarshal(metadata, &existingMetadata); err != nil {
			return err
		}
		metab, err := db.mergeMetas(existingMetadata, updates)
		if err != nil {
			return err
		}
		return tx.Put([]byte("info/metadata"), metab, nil)
	})
}

func (db *levelContainer) MergeItems(records []*ObjectRecord, remoteID string) error {
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
	return db.Update(func(tx *leveldb.Transaction) error {
		ocb, err := tx.Get([]byte("info/object_count"), nil)
		if err != nil {
			return err
		}
		bub, err := tx.Get([]byte("info/bytes_used"), nil)
		if err != nil {
			return err
		}
		hsb, err := tx.Get([]byte("info/hash"), nil)
		if err != nil {
			return err
		}
		mrb, err := tx.Get([]byte("info/max_rowid"), nil)
		if err != nil {
			return err
		}
		objectCount, err := strconv.ParseInt(string(ocb), 10, 64)
		if err != nil {
			return err
		}
		bytesUsed, err := strconv.ParseInt(string(bub), 10, 64)
		if err != nil {
			return err
		}
		maxLocalRowid, err := strconv.ParseInt(string(mrb), 10, 64)
		if err != nil {
			return err
		}
		hash := string(hsb)

		for _, name := range names {
			objb, err := tx.Get([]byte("objects/1/"+name), nil)
			if err != nil {
				objb, err = tx.Get([]byte("objects/0/"+name), nil)
				if err != nil {
					continue
				}
			}
			var obj ObjectRecord
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
				tx.Delete([]byte(fmt.Sprintf("rowids/%019d", current.Rowid)), nil)
				if current.Deleted != 1 {
					objectCount -= 1
					bytesUsed -= current.Size
				}
				if alreadyIn, ok := toAdd[rid]; !ok || record.CreatedAt > alreadyIn.CreatedAt {
					toAdd[rid] = record
				}
			}
		}
		var maxIncomingRowid int64
		for _, record := range toAdd {
			if record.Rowid > maxIncomingRowid {
				maxIncomingRowid = record.Rowid
			}
			maxLocalRowid += 1
			record.Rowid = int64(maxLocalRowid)
			objb, err := msgpack.Marshal(record)
			if err != nil {
				return err
			}
			recKey := []byte(fmt.Sprintf("objects/%d/%s", record.Deleted, record.Name))
			tx.Put(recKey, objb, nil)
			rowidKey := []byte(fmt.Sprintf("rowids/%019d", record.Rowid))
			tx.Put(rowidKey, recKey, nil)
			hash = chexor(hash, record.Name, record.CreatedAt)
			if record.Deleted != 1 {
				objectCount += 1
				bytesUsed += record.Size
			}
		}
		tx.Put([]byte("info/object_count"), []byte(strconv.FormatInt(objectCount, 10)), nil)
		tx.Put([]byte("info/bytes_used"), []byte(strconv.FormatInt(bytesUsed, 10)), nil)
		tx.Put([]byte("info/max_rowid"), []byte(strconv.FormatInt(maxLocalRowid, 10)), nil)
		tx.Put([]byte("info/hash"), []byte(hash), nil)
		if remoteID != "" {
			tx.Put([]byte("incomingsyncs/"+remoteID), []byte(strconv.FormatInt(maxIncomingRowid, 10)), nil)
		}
		return nil
	})
}

func (db *levelContainer) ItemsSince(start int64, count int) ([]*ObjectRecord, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	results := []*ObjectRecord{}
	startKey := []byte(fmt.Sprintf("rowids/%019d!", start))
	iter := db.NewIterator(&util.Range{Start: []byte(startKey), Limit: []byte("rowids/\xFF")}, nil)
	defer iter.Release()
	for iter.Next() {
		k := iter.Key()
		v := iter.Value()
		if k == nil || v == nil {
			break
		}
		objb, err := db.Get([]byte("objects/0/"+string(v)), nil)
		if err != nil {
			continue
		}
		var obj ObjectRecord
		if err := msgpack.Unmarshal(objb, &obj); err != nil {
			return nil, err
		}
		results = append(results, &obj)
	}
	return results, iter.Error()
}

func (db *levelContainer) MergeSyncTable(records []*SyncRecord) error {
	if err := db.connect(); err != nil {
		return err
	}
	return db.Update(func(tx *leveldb.Transaction) error {
		for _, rec := range records {
			val, err := tx.Get([]byte("incomingsyncs/"+rec.RemoteID), nil)
			if err == nil {
				current, err := strconv.ParseInt(string(val), 10, 64)
				if err != nil {
					return err
				}
				if current >= rec.SyncPoint {
					continue
				}
			}
			tx.Put([]byte("incomingsyncs/"+rec.RemoteID), []byte(strconv.FormatInt(rec.SyncPoint, 10)), nil)
		}
		return nil
	})
}

func (db *levelContainer) SyncTable() ([]*SyncRecord, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	results := []*SyncRecord{}
	iter := db.NewIterator(util.BytesPrefix([]byte("incomingsyncs/")), nil)
	defer iter.Release()
	for iter.Next() {
		syncPoint, err := strconv.ParseInt(string(iter.Value()), 10, 64)
		if err != nil {
			return nil, err
		}
		results = append(results, &SyncRecord{RemoteID: string(iter.Key()[14:]), SyncPoint: syncPoint})
	}
	if err := iter.Error(); err != nil {
		return nil, err
	}
	id, err := db.Get([]byte("info/id"), nil)
	if err != nil {
		return nil, err
	}
	maxRowb, err := db.Get([]byte("info/max_rowid"), nil)
	if err != nil {
		return nil, err
	}
	maxRow, err := strconv.ParseInt(string(maxRowb), 10, 64)
	if err != nil {
		return nil, err
	}
	results = append(results, &SyncRecord{RemoteID: string(id), SyncPoint: maxRow})
	return results, nil
}

func (db *levelContainer) SyncRemoteData(maxRow int64, hash, id, createdAt, putTimestamp, deleteTimestamp, metadata string) (*ContainerInfo, error) {
	if err := db.connect(); err != nil {
		return nil, err
	}
	var localPoint int64
	err := db.Update(func(tx *leveldb.Transaction) error {
		var err error
		hsb, err := tx.Get([]byte("info/hash"), nil)
		if err != nil {
			return err
		}
		ptb, err := tx.Get([]byte("info/put_timestamp"), nil)
		if err != nil {
			return err
		}
		dtb, err := tx.Get([]byte("info/delete_timestamp"), nil)
		if err != nil {
			return err
		}
		cab, err := tx.Get([]byte("info/created_at"), nil)
		if err != nil {
			return err
		}
		metb, err := tx.Get([]byte("info/metadata"), nil)
		if err != nil {
			return err
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
		tx.Put([]byte("info/metadata"), metb, nil)
		if createdAt < string(cab) {
			tx.Put([]byte("info/created_at"), []byte(createdAt), nil)
		}
		if putTimestamp > string(ptb) {
			tx.Put([]byte("info/put_timestamp"), []byte(putTimestamp), nil)
		}
		if deleteTimestamp > string(dtb) {
			tx.Put([]byte("info/delete_timestamp"), []byte(deleteTimestamp), nil)
		}
		if string(hsb) == hash {
			tx.Put([]byte("incomingsyncs/"+id), []byte(strconv.FormatInt(maxRow, 10)), nil)
		}
		pb, err := tx.Get([]byte("incomingsyncs/"+id), nil)
		if err != nil {
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

func (db *levelContainer) ID() string {
	return db.ringhash
}

func (db *levelContainer) RingHash() string {
	return db.ringhash
}

func (db *levelContainer) NewID() error {
	if err := db.connect(); err != nil {
		return err
	}
	defer db.invalidateCache()
	return db.Update(func(tx *leveldb.Transaction) error {
		tx.Put([]byte("info/id"), []byte(hummingbird.UUID()), nil)
		return nil
	})
}

func (db *levelContainer) OpenDatabaseFile() (*os.File, func(), error) {
	return nil, nil, fmt.Errorf("Unimplemented")
}

func (db *levelContainer) CleanupTombstones(reclaimAge int64) error {
	if err := db.connect(); err != nil {
		return err
	}
	now := float64(time.Now().UnixNano()) / 1000000000.0
	reclaimTimestamp := hummingbird.CanonicalTimestamp(now - float64(reclaimAge))
	return db.Update(func(tx *leveldb.Transaction) error {
		hsb, err := tx.Get([]byte("info/hash"), nil)
		if err != nil {
			return errors.New("Invalid hash")
		}
		hash := string(hsb)
		iter := tx.NewIterator(util.BytesPrefix([]byte("objects/1/")), nil)
		defer iter.Release()
		for iter.Next() {
			var obj ObjectRecord
			if err := msgpack.Unmarshal(iter.Value(), &obj); err != nil {
				return err
			}
			if obj.CreatedAt < reclaimTimestamp {
				tx.Delete(iter.Key(), nil)
				tx.Delete([]byte(fmt.Sprintf("rowids/%019d", obj.Rowid)), nil)
			}
			hash = chexor(hash, obj.Name, obj.CreatedAt)
		}
		if err := iter.Error(); err != nil {
			return err
		}
		tx.Put([]byte("info/hash"), []byte(hash), nil)
		return nil
	})
}

func (db *levelContainer) flushAlreadyLocked() error {
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

func (db *levelContainer) flush() error {
	lock, err := hummingbird.LockPath(filepath.Dir(db.containerFile), 10*time.Second)
	if err != nil {
		return err
	}
	defer lock.Close()
	return db.flushAlreadyLocked()
}

func (db *levelContainer) addObject(name string, timestamp string, size int64, contentType string, etag string, deleted int, storagePolicyIndex int) error {
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
func (db *levelContainer) PutObject(name string, timestamp string, size int64, contentType string, etag string, storagePolicyIndex int) error {
	return db.addObject(name, timestamp, size, contentType, etag, 0, storagePolicyIndex)
}

// DeleteObject removes an object from the container, by way of pending file.
func (db *levelContainer) DeleteObject(name string, timestamp string, storagePolicyIndex int) error {
	return db.addObject(name, timestamp, 0, "", "", 1, storagePolicyIndex)
}

// CheckSyncLink does nothing for now, as the level container does not yet support container sync.
func (db *levelContainer) CheckSyncLink() error {
	return nil
}

func levelCreateExistingContainer(db Container, putTimestamp string, newMetadata map[string][]string, policyIndex, defaultPolicyIndex int) (bool, error) {
	cdb, ok := db.(*levelContainer)
	if !ok {
		return false, errors.New("Unable to work with non-levelContainer")
	}
	created := false
	defer cdb.invalidateCache()
	err := cdb.Update(func(tx *leveldb.Transaction) error {
		bDeleteTimestamp, err := tx.Get([]byte("info/delete_timestamp"), nil)
		if err != nil {
			return err
		}
		bPutTimestamp, err := tx.Get([]byte("info/put_timestamp"), nil)
		if err != nil {
			return err
		}
		bMetadata, err := tx.Get([]byte("info/metadata"), nil)
		if err != nil {
			return err
		}
		bPolicyIndex, err := tx.Get([]byte("info/storage_policy_index"), nil)
		if err != nil {
			return err
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
		tx.Put([]byte("info/put_timestamp"), []byte(putTimestamp), nil)
		tx.Put([]byte("info/storage_policy_index"), []byte(strconv.FormatInt(int64(policyIndex), 10)), nil)
		tx.Put([]byte("info/metadata"), metab, nil)
		created = (cDeleteTimestamp > cPutTimestamp && putTimestamp > cDeleteTimestamp)
		return nil
	})
	return created, err
}

func levelCreateContainer(containerFile string, account string, container string, putTimestamp string, metadata map[string][]string, policyIndex int) error {
	hashDir := filepath.Dir(containerFile)
	if err := os.MkdirAll(hashDir, 0755); err != nil {
		return err
	}
	db, err := leveldb.OpenFile(containerFile, nil)
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
	for k, v := range infoVals {
		if err := db.Put([]byte("info/"+k), []byte(v), nil); err != nil {
			return err
		}
	}
	return nil
	return err
}

func levelOpenContainer(containerFile string) (*levelContainer, error) {
	if _, err := os.Stat(containerFile); os.IsNotExist(err) {
		return nil, errors.New("Does not exist.")
	}
	ringhash := filepath.Base(filepath.Dir(containerFile))
	db := &levelContainer{containerFile: containerFile, ringhash: ringhash}
	return db, nil
}

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
	"crypto/md5"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"

	"github.com/openstack/swift/go/hummingbird"
)

const METADATA_CHUNK_SIZE = 65536

func GetXAttr(fileNameOrFd interface{}, attr string, value []byte) (int, error) {
	switch v := fileNameOrFd.(type) {
	case string:
		return syscall.Getxattr(v, attr, value)
	case uintptr:
		return hummingbird.FGetXattr(v, attr, value)
	}
	return 0, &hummingbird.BackendError{Err: errors.New("Invalid fileNameOrFd"), Code: hummingbird.UnhandledError}
}

func RawReadMetadata(fileNameOrFd interface{}) ([]byte, error) {
	var pickledMetadata []byte
	offset := 0
	for index := 0; ; index += 1 {
		var metadataName string
		// get name of next xattr
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		// get size of xattr
		length, err := GetXAttr(fileNameOrFd, metadataName, nil)
		if err != nil || length <= 0 {
			break
		}
		// grow buffer to hold xattr
		for cap(pickledMetadata) < offset+length {
			pickledMetadata = append(pickledMetadata, 0)
		}
		pickledMetadata = pickledMetadata[0 : offset+length]
		if _, err := GetXAttr(fileNameOrFd, metadataName, pickledMetadata[offset:]); err != nil {
			return nil, err
		}
		offset += length
	}
	return pickledMetadata, nil
}

func ReadMetadata(fileNameOrFd interface{}) (map[string]string, error) {
	pickledMetadata, err := RawReadMetadata(fileNameOrFd)
	if err != nil {
		return nil, err
	}
	v, err := hummingbird.PickleLoads(pickledMetadata)
	if err != nil {
		return nil, err
	}
	if v, ok := v.(map[interface{}]interface{}); ok {
		metadata := make(map[string]string, len(v))
		for mk, mv := range v {
			var mks, mvs string
			if mks, ok = mk.(string); !ok {
				return nil, fmt.Errorf("Metadata key not string: %v", mk)
			} else if mvs, ok = mv.(string); !ok {
				return nil, fmt.Errorf("Metadata value not string: %v", mv)
			}
			metadata[mks] = mvs
		}
		return metadata, nil
	}
	return nil, fmt.Errorf("Unpickled metadata not correct type")
}

func RawWriteMetadata(fd uintptr, buf []byte) error {
	for index := 0; len(buf) > 0; index++ {
		var metadataName string
		if index == 0 {
			metadataName = "user.swift.metadata"
		} else {
			metadataName = "user.swift.metadata" + strconv.Itoa(index)
		}
		writelen := METADATA_CHUNK_SIZE
		if len(buf) < writelen {
			writelen = len(buf)
		}
		if _, err := hummingbird.FSetXattr(fd, metadataName, buf[0:writelen]); err != nil {
			return err
		}
		buf = buf[writelen:len(buf)]
	}
	return nil
}

func WriteMetadata(fd uintptr, v map[string]string) error {
	return RawWriteMetadata(fd, hummingbird.PickleDumps(v))
}

func QuarantineHash(hashDir string) error {
	// FYI- this does not invalidate the hash like swift's version. Please
	// do that yourself
	hash := filepath.Base(hashDir)
	//          drive        objects      partition    suffix       hash
	driveDir := filepath.Dir(filepath.Dir(filepath.Dir(filepath.Dir(hashDir))))
	// TODO: this will need to be slightly more complicated once policies
	quarantineDir := filepath.Join(driveDir, "quarantined", "objects")
	if err := os.MkdirAll(quarantineDir, 0755); err != nil {
		return err
	}
	destDir := filepath.Join(quarantineDir, hash+"-"+hummingbird.UUID())
	if err := os.Rename(hashDir, destDir); err != nil {
		return err
	}
	return nil
}

func HashCleanupListDir(hashDir string, logger hummingbird.LoggingContext) ([]string, *hummingbird.BackendError) {
	fileList, err := hummingbird.ReadDirNames(hashDir)
	returnList := []string{}
	if err != nil {
		if os.IsNotExist(err) {
			return returnList, nil
		}
		if hummingbird.IsNotDir(err) {
			return returnList, &hummingbird.BackendError{Err: err, Code: hummingbird.PathNotDirErrorCode}
		}
		return returnList, &hummingbird.BackendError{Err: err, Code: hummingbird.OsErrorCode}
	}
	deleteRest := false
	deleteRestMeta := false
	if len(fileList) == 1 {
		filename := fileList[0]
		returnList = append(returnList, filename)
	} else {
		for index := len(fileList) - 1; index >= 0; index-- {
			filename := fileList[index]
			if deleteRest {
				os.RemoveAll(hashDir + "/" + filename)
			} else {
				if strings.HasSuffix(filename, ".meta") {
					if deleteRestMeta {
						os.RemoveAll(hashDir + "/" + filename)
						continue
					}
					deleteRestMeta = true
				}
				if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
					deleteRest = true
				}
				returnList = append(returnList, filename)
			}
		}
	}
	return returnList, nil
}

func ObjHashDir(vars map[string]string, driveRoot string, hashPathPrefix string, hashPathSuffix string) string {
	h := md5.New()
	io.WriteString(h, hashPathPrefix+"/"+vars["account"]+"/"+vars["container"]+"/"+vars["obj"]+hashPathSuffix)
	hexHash := hex.EncodeToString(h.Sum(nil))
	suffix := hexHash[29:32]
	return filepath.Join(driveRoot, vars["device"], "objects", vars["partition"], suffix, hexHash)
}

func ObjectFiles(directory string) (string, string) {
	fileList, err := hummingbird.ReadDirNames(directory)
	metaFile := ""
	if err != nil {
		return "", ""
	}
	for index := len(fileList) - 1; index >= 0; index-- {
		filename := fileList[index]
		if strings.HasSuffix(filename, ".meta") {
			metaFile = filename
		}
		if strings.HasSuffix(filename, ".ts") || strings.HasSuffix(filename, ".data") {
			if metaFile != "" {
				return filepath.Join(directory, filename), filepath.Join(directory, metaFile)
			} else {
				return filepath.Join(directory, filename), ""
			}
		}
	}
	return "", ""
}

func ObjTempFile(vars map[string]string, driveRoot, prefix string) (*os.File, error) {
	tempDir := driveRoot + "/" + vars["device"] + "/" + "tmp"
	if err := os.MkdirAll(tempDir, 0755); err != nil {
		return nil, err
	}
	return ioutil.TempFile(tempDir, prefix)
}

func applyMetaFile(metaFile string, datafileMetadata map[string]string) (map[string]string, error) {
	if metadata, err := ReadMetadata(metaFile); err != nil {
		return nil, err
	} else {
		for k, v := range datafileMetadata {
			if k == "Content-Length" || k == "Content-Type" || k == "deleted" || k == "ETag" || strings.HasPrefix(k, "X-Object-Sysmeta-") {
				metadata[k] = v
			}
		}
		return metadata, nil
	}
}

func OpenObjectMetadata(fd uintptr, metaFile string) (map[string]string, error) {
	datafileMetadata, err := ReadMetadata(fd)
	if err != nil {
		return nil, err
	}
	if metaFile != "" {
		return applyMetaFile(metaFile, datafileMetadata)
	}
	return datafileMetadata, nil
}

func ObjectMetadata(dataFile string, metaFile string) (map[string]string, error) {
	datafileMetadata, err := ReadMetadata(dataFile)
	if err != nil {
		return nil, err
	}
	if metaFile != "" {
		return applyMetaFile(metaFile, datafileMetadata)
	}
	return datafileMetadata, nil
}

func FreeDiskSpace(fd uintptr) (int64, error) {
	var st syscall.Statfs_t
	if err := syscall.Fstatfs(int(fd), &st); err != nil {
		return 0, err
	} else {
		return int64(st.Frsize) * int64(st.Bavail), nil
	}
}

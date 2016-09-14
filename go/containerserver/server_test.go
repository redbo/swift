//  Copyright (c) 2016 Rackspace
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

package containerserver

import (
	"encoding/json"
	"flag"
	"net/http"
	"testing"

	"github.com/openstack/swift/go/hummingbird"
	"github.com/stretchr/testify/require"
)

func TestFormatTimestamp(t *testing.T) {
	require.Equal(t, "0000000000.00000", formatTimestamp("something"))
	require.Equal(t, "0000000001.00000", formatTimestamp("1"))
	require.Equal(t, "1000000000.00000", formatTimestamp("1000000000"))
	require.Equal(t, "1000000000.00000", formatTimestamp("1000000000.00000"))
}

func TestContainerGetNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("GET", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)
}

func TestContainerDeleteNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)
}

func TestContainerPostDeleted(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)
}

func TestContainerPostNotFound(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("POST", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)
}

func TestContainerPostBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("POST", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", "invalid")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)
}

func TestContainerPutBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)
}

func TestContainerDeleteBadTimestamp(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	req.Header.Set("X-Timestamp", "invalid")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)
}

func TestContainerPutExisting(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "2000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 202, rsp.status)
}

func TestContainerPutPolicyConflict(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "2000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 409, rsp.status)
}

func TestContainerPutHead(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
	require.Equal(t, "0", rsp.header.Get("X-Container-Object-Count"))
	require.Equal(t, "0", rsp.header.Get("X-Container-Bytes-Used"))
	require.Equal(t, "2", rsp.header.Get("X-Backend-Storage-Policy-Index"))
	require.Equal(t, "1000000000.00001", rsp.header.Get("X-Put-Timestamp"))
}

func TestContainerPutNoPolicy(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
	require.Equal(t, "0", rsp.header.Get("X-Backend-Storage-Policy-Index"))
}

func TestContainerDeleteNotEmpty(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := makeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
		req.Header.Set("X-Content-Type", "application/octet-stream")
		req.Header.Set("X-Size", "2")
		req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.status)
	}

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 409, rsp.status)
}

func TestContainerPutObjectsGet(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := makeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
		req.Header.Set("X-Content-Type", "application/octet-stream")
		req.Header.Set("X-Size", "2")
		req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.status)
	}

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c?format=json", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.status)
	require.Equal(t, "application/json; charset=utf-8", rsp.header.Get("Content-Type"))
	require.Equal(t, "3", rsp.header.Get("X-Container-Object-Count"))
	require.Equal(t, "6", rsp.header.Get("X-Container-Bytes-Used"))
	var data []ObjectListingRecord
	require.Nil(t, json.Unmarshal(rsp.body.Bytes(), &data))
	require.Equal(t, 3, len(data))
	require.Equal(t, "1", data[0].Name)
	require.Equal(t, "2", data[1].Name)
	require.Equal(t, "3", data[2].Name)
	require.Equal(t, int64(2), data[2].Size)
	require.Equal(t, "d41d8cd98f00b204e9800998ecf8427e", data[2].ETag)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c", nil)
	req.Header.Set("Accept", "application/json")
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.status)
	require.Equal(t, "application/json; charset=utf-8", rsp.header.Get("Content-Type"))

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.status)
	require.Equal(t, "1\n2\n3\n", rsp.body.String())

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c?format=xml", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.status)
	require.Equal(t, "application/xml; charset=utf-8", rsp.header.Get("Content-Type"))
	// TODO parse and validate xml.  or maybe we won't do that.
}

func TestContainerPutObjectsFails(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	server.containerEngine = fakeContainerEngine{}

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 500, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 500, rsp.status)
}

func TestContainerGetTextEmpty(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "100000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("GET", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
}

func TestContainerPutObjectBadRequests(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "invalid")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)
}

func TestContainerPutDeleteObjectsGet(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	for _, object := range []string{"1", "2", "3"} {
		rsp := makeCaptureResponse()
		req, err := http.NewRequest("PUT", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
		req.Header.Set("X-Content-Type", "application/octet-stream")
		req.Header.Set("X-Size", "2")
		req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 201, rsp.status)
	}

	for _, object := range []string{"1", "2", "3"} {
		rsp := makeCaptureResponse()
		req, err := http.NewRequest("DELETE", "/device/1/a/c/"+object, nil)
		require.Nil(t, err)
		req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
		handler.ServeHTTP(rsp, req)
		require.Equal(t, 204, rsp.status)
	}

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
	require.Equal(t, "0", rsp.header.Get("X-Container-Object-Count"))
	require.Equal(t, "0", rsp.header.Get("X-Container-Bytes-Used"))
}

func TestContainerMetadata(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Meta-First", "1")
	req.Header.Set("X-Container-Meta-Second", "2")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Container-Meta-First", "!")
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Container-Meta-Second", "@")
	req.Header.Set("X-Timestamp", hummingbird.CanonicalTimestamp(1))
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
	require.Equal(t, "!", rsp.header.Get("X-Container-Meta-First"))
	require.Equal(t, "2", rsp.header.Get("X-Container-Meta-Second"))
}

func TestContainerDelete(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.CanonicalTimestamp(1))
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)
}

func TestHealthcheck(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("GET", "/healthcheck", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.status)
	require.Equal(t, "OK", rsp.body.String())
}

func TestDiskUsage(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()

	server.diskInUse = hummingbird.NewKeyedLimit(2, 8)
	server.diskInUse.Acquire("sda", false)
	rsp := makeCaptureResponse()
	req, err := http.NewRequest("GET", "/diskusage", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 200, rsp.status)
	expected, err := server.diskInUse.MarshalJSON()
	require.Nil(t, err)
	require.Equal(t, string(expected), string(rsp.body.Bytes()))
}

func TestGetServer(t *testing.T) {
	oldgethash := GetHashPrefixAndSuffix
	oldgetsync := GetSyncRealms
	defer func() {
		GetHashPrefixAndSuffix = oldgethash
		GetSyncRealms = oldgetsync
	}()
	GetHashPrefixAndSuffix = func() (string, string, error) {
		return "changeme", "changeme", nil
	}
	GetSyncRealms = func() hummingbird.SyncRealmList {
		return hummingbird.SyncRealmList(map[string]hummingbird.SyncRealm{})
	}

	configString := "[app:container-server]\ndevices=whatever\nmount_check=false\nbind_ip=127.0.0.2\nbind_port=1000\nlog_level=INFO\n"
	conf, err := hummingbird.StringConfig(configString)
	require.Nil(t, err)
	bindIP, bindPort, s, logger, err := GetServer(conf, &flag.FlagSet{})
	require.Nil(t, err)
	server, ok := s.(*ContainerServer)
	require.True(t, ok)
	require.Equal(t, "127.0.0.2", bindIP)
	require.Equal(t, 1000, bindPort)
	require.NotNil(t, logger)
	require.Equal(t, "whatever", server.driveRoot)
	require.Equal(t, "INFO", server.logLevel)
	require.False(t, server.checkMounts)
	require.NotNil(t, server.updateClient)
	require.NotNil(t, server.containerEngine)
}

func TestContainerAutoCreateOnPut(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/.a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/.a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
}

func TestContainerAutoCreateOnDelete(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("DELETE", "/device/1/.a/c/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("HEAD", "/device/1/.a/c", nil)
	require.Nil(t, err)
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
}

func TestContainerNotFoundOnPut(t *testing.T) {
	handler, cleanup, err := makeTestServer()
	require.Nil(t, err)
	defer cleanup()

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/cX/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	req.Header.Set("X-Content-Type", "application/octet-stream")
	req.Header.Set("X-Size", "2")
	req.Header.Set("X-Etag", "d41d8cd98f00b204e9800998ecf8427e")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("DELETE", "/device/1/a/cY/o", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", hummingbird.GetTimestamp())
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 404, rsp.status)
}

func TestContainerVerifySync(t *testing.T) {
	server, handler, cleanup, err := makeTestServer2()
	require.Nil(t, err)
	defer cleanup()
	server.syncRealms = hummingbird.SyncRealmList(map[string]hummingbird.SyncRealm{
		"realm1": hummingbird.SyncRealm{
			Name:     "realm1",
			Key1:     "somekey",
			Key2:     "someotherkey",
			Clusters: map[string]string{"cluster1": "http://some/cluster/url"},
		},
	})

	rsp := makeCaptureResponse()
	req, err := http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm2/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("PUT", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm1/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 201, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm2/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 400, rsp.status)

	rsp = makeCaptureResponse()
	req, err = http.NewRequest("POST", "/device/1/a/c", nil)
	require.Nil(t, err)
	req.Header.Set("X-Timestamp", "1000000000.00001")
	req.Header.Set("X-Backend-Storage-Policy-Index", "0")
	req.Header.Set("X-Container-Sync-To", "//realm1/cluster1/account/container")
	handler.ServeHTTP(rsp, req)
	require.Equal(t, 204, rsp.status)
}

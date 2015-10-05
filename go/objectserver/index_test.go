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
	"testing"

	"github.com/stretchr/testify/require"
)

func TestIndexFileInfo(t *testing.T) {
	suffix, objHash, timestamp, ext := fileInfo("/srv/node/sda/objects/1/fff/00000000000000000000000000000000/1234567.1234.data")
	require.Equal(t, "fff", suffix)
	require.Equal(t, "00000000000000000000000000000000", objHash)
	require.Equal(t, "1234567.1234", timestamp)
	require.Equal(t, "data", ext)
}

func TestSuffixHash(t *testing.T) {
	sh := &suffixHash{}
	sh.Step("/srv/node/sda/objects/1/abc/00000000000000000000000000000abc/00000.00000.data")
	sh.Step("/srv/node/sda/objects/1/abc/00000000000000000000000000000abc/55555.55555.meta")
	sh.Step("/srv/node/sda/objects/1/abc/50000000000000000000000000000abc/00000.00000.data")
	sh.Step("/srv/node/sda/objects/1/abc/50000000000000000000000000000abc/55555.55555.meta")
	sh.Step("/srv/node/sda/objects/1/abc/90000000000000000000000000000abc/00000.00000.data")
	sh.Step("/srv/node/sda/objects/1/abc/90000000000000000000000000000abc/55555.55555.meta")
	require.Equal(t, "6e8a8c43be4a5cadc63d578cb21364c5", sh.Done())
}
